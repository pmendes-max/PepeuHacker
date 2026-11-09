"""HubSpot Engagements API — pulls calls, emails and meetings for SDR owners.

Auth: HUBSPOT_TOKEN env var (Private App access token, formato pat-na2-...).
Cache: parquet em ./cache/hs_*.parquet (TTL 1h por padrão).
Layer: foco em camada de CONTATO (associações engagement→contact via v4 batch).

3 object types puxados separados, depois agregados:
  - calls    (objectType=calls,    date_property=hs_timestamp,         status=COMPLETED)
  - emails   (objectType=emails,   date_property=hs_timestamp,         direction=OUTBOUND)
  - meetings (objectType=meetings, date_property=hs_meeting_start_time, outcome=COMPLETED)

Uso:
    import hubspot as hs
    if hs.is_configured():
        data = hs.effort_summary(team_owners, '2026-04-01', '2026-04-28')
"""
from __future__ import annotations

import os
import time
from pathlib import Path

import pandas as pd
import requests

API = "https://api.hubapi.com"
CACHE_DIR = Path(__file__).parent / "cache"
CACHE_TTL = 3600  # 1h

# Configuração de cada object type que a gente puxa.
ENGAGEMENT_CONFIG = {
    "calls": {
        "object_type":   "calls",
        "date_property": "hs_timestamp",
        "properties": [
            "hs_timestamp", "hs_call_status", "hs_call_direction",
            "hs_call_duration", "hs_call_disposition", "hs_call_title",
            "hubspot_owner_id", "hs_createdate",
        ],
        "extra_filters": [
            {"propertyName": "hs_call_status", "operator": "EQ", "value": "COMPLETED"},
        ],
    },
    "emails": {
        "object_type":   "emails",
        "date_property": "hs_timestamp",
        "properties": [
            "hs_timestamp", "hs_email_direction", "hs_email_status",
            "hs_email_subject", "hubspot_owner_id", "hs_createdate",
        ],
        # OUTBOUND_EMAIL na API = email enviado pelo dono (esforço do SDR).
        # INBOUND_EMAIL é resposta do contato — não conta como esforço.
        "extra_filters": [
            {"propertyName": "hs_email_direction", "operator": "EQ", "value": "EMAIL"},
        ],
    },
    "meetings": {
        "object_type":   "meetings",
        "date_property": "hs_meeting_start_time",
        "properties": [
            "hs_meeting_start_time", "hs_meeting_end_time", "hs_meeting_outcome",
            "hs_meeting_title", "hubspot_owner_id", "hs_createdate",
        ],
        "extra_filters": [
            {"propertyName": "hs_meeting_outcome", "operator": "EQ", "value": "COMPLETED"},
        ],
    },
}


class HubSpotError(Exception):
    pass


def token() -> str | None:
    return os.environ.get("HUBSPOT_TOKEN")


def is_configured() -> bool:
    return bool(token())


def _headers() -> dict:
    t = token()
    if not t:
        raise HubSpotError(
            "HUBSPOT_TOKEN não configurado. Cria um Private App no HubSpot com escopos "
            "crm.objects.calls.read, crm.objects.emails.read, crm.objects.meetings.read, "
            "crm.objects.contacts.read, crm.objects.owners.read, crm.objects.deals.read e cola o token em "
            "~/.config/hubspot/env (formato: export HUBSPOT_TOKEN='pat-na2-...')."
        )
    return {"Authorization": f"Bearer {t}", "Content-Type": "application/json"}


def _cache_path(key: str) -> Path:
    CACHE_DIR.mkdir(exist_ok=True)
    safe = "".join(c if c.isalnum() or c in "_-" else "_" for c in key)
    return CACHE_DIR / f"hs_{safe}.parquet"


def _cache_read(key: str, ttl: int = CACHE_TTL) -> pd.DataFrame | None:
    p = _cache_path(key)
    if not p.exists() or time.time() - p.stat().st_mtime > ttl:
        return None
    try:
        return pd.read_parquet(p)
    except Exception:
        return None


def _cache_write(key: str, df: pd.DataFrame) -> None:
    df.to_parquet(_cache_path(key))


def _post(path: str, body: dict, retry: int = 0) -> dict:
    """POST com retry em rate-limit (429) e 5xx transiente."""
    r = requests.post(f"{API}{path}", headers=_headers(), json=body, timeout=30)
    if r.status_code == 429:
        if retry >= 3:
            raise HubSpotError(f"Rate-limited em {path} após 3 retries")
        time.sleep(int(r.headers.get("Retry-After", "2")))
        return _post(path, body, retry + 1)
    if r.status_code >= 500 and retry < 2:
        time.sleep(2 ** retry)
        return _post(path, body, retry + 1)
    if r.status_code >= 400:
        raise HubSpotError(f"HubSpot {r.status_code} em {path}: {r.text[:300]}")
    return r.json()


# ============================================================
# Engagement search (genérico — calls/emails/meetings)
# ============================================================

def _search_engagement(
    bucket: str,
    owner_ids: list[int],
    date_from: str,
    date_to: str,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Search genérico em qualquer objeto da config. Retorna DataFrame normalizado."""
    cfg = ENGAGEMENT_CONFIG[bucket]
    cache_key = f"{bucket}_{'_'.join(str(o) for o in sorted(owner_ids))}_{date_from}_{date_to}"
    if not force_refresh:
        cached = _cache_read(cache_key)
        if cached is not None:
            return cached

    base_filters = [
        {"propertyName": "hubspot_owner_id", "operator": "IN",
         "values": [str(o) for o in owner_ids]},
        {"propertyName": cfg["date_property"], "operator": "GTE",
         "value": f"{date_from}T00:00:00.000Z"},
        {"propertyName": cfg["date_property"], "operator": "LTE",
         "value": f"{date_to}T23:59:59.999Z"},
    ] + cfg["extra_filters"]

    body = {
        "filterGroups": [{"filters": base_filters}],
        "properties":  cfg["properties"],
        "limit":       200,
        "sorts": [{"propertyName": cfg["date_property"], "direction": "DESCENDING"}],
    }

    rows = []
    after = None
    page = 0
    while True:
        page += 1
        if after:
            body["after"] = after
        resp = _post(f"/crm/v3/objects/{cfg['object_type']}/search", body)
        for r in resp.get("results", []):
            p = r.get("properties", {})
            rows.append({
                "engagement_id": r.get("id"),
                "owner_id":      int(p.get("hubspot_owner_id") or 0),
                "bucket":        bucket,  # 'calls' / 'emails' / 'meetings'
                "date":          p.get(cfg["date_property"]),
                "subject":       p.get("hs_call_title")
                                  or p.get("hs_email_subject")
                                  or p.get("hs_meeting_title")
                                  or "",
                "extra":         {k: v for k, v in p.items()
                                  if k not in ("hubspot_owner_id",
                                                cfg["date_property"])},
            })
        paging = resp.get("paging", {}).get("next", {})
        after = paging.get("after")
        if not after or page > 100:
            break

    df = pd.DataFrame(rows)
    if len(df):
        df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    _cache_write(cache_key, df)
    return df


def calls_for_owners(owner_ids, date_from, date_to, force_refresh=False):
    return _search_engagement("calls", owner_ids, date_from, date_to, force_refresh)


def emails_for_owners(owner_ids, date_from, date_to, force_refresh=False):
    return _search_engagement("emails", owner_ids, date_from, date_to, force_refresh)


def meetings_for_owners(owner_ids, date_from, date_to, force_refresh=False):
    return _search_engagement("meetings", owner_ids, date_from, date_to, force_refresh)


# ============================================================
# Associations: engagement → contact
# ============================================================

def engagement_contact_associations(
    engagement_type: str,
    engagement_ids: list[str],
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Para cada engagement (call/email/meeting) ID, retorna contatos associados."""
    if not engagement_ids:
        return pd.DataFrame(columns=["engagement_id", "contact_id"])

    cache_key = (f"assoc_{engagement_type}_contacts_"
                 f"{hash(tuple(sorted(engagement_ids))) & 0xFFFFFFFF}")
    if not force_refresh:
        cached = _cache_read(cache_key)
        if cached is not None:
            return cached

    rows = []
    for i in range(0, len(engagement_ids), 100):
        chunk = engagement_ids[i:i + 100]
        body = {"inputs": [{"id": eid} for eid in chunk]}
        resp = _post(
            f"/crm/v4/associations/{engagement_type}/contacts/batch/read", body)
        for item in resp.get("results", []):
            eid = item.get("from", {}).get("id")
            for to in item.get("to", []):
                rows.append({"engagement_id": eid,
                             "contact_id": str(to.get("toObjectId"))})

    df = pd.DataFrame(rows)
    _cache_write(cache_key, df)
    return df


# ============================================================
# Contacts batch
# ============================================================

def contacts_by_id(contact_ids: list[str], force_refresh: bool = False) -> pd.DataFrame:
    if not contact_ids:
        return pd.DataFrame()

    contact_ids = [str(c) for c in contact_ids]
    cache_key = f"contacts_{hash(tuple(sorted(contact_ids))) & 0xFFFFFFFF}"
    if not force_refresh:
        cached = _cache_read(cache_key)
        if cached is not None:
            return cached

    rows = []
    for i in range(0, len(contact_ids), 100):
        chunk = contact_ids[i:i + 100]
        body = {
            "inputs": [{"id": cid} for cid in chunk],
            "properties": ["firstname", "lastname", "company", "email", "jobtitle"],
        }
        resp = _post("/crm/v3/objects/contacts/batch/read", body)
        for r in resp.get("results", []):
            p = r.get("properties", {})
            full = f"{p.get('firstname','') or ''} {p.get('lastname','') or ''}".strip()
            rows.append({
                "contact_id": r.get("id"),
                "name":       full or "(sem nome)",
                "company":    p.get("company") or "",
                "email":      p.get("email") or "",
                "jobtitle":   p.get("jobtitle") or "",
            })

    df = pd.DataFrame(rows)
    _cache_write(cache_key, df)
    return df


# ============================================================
# High-level: agg por SDR + por contato + série diária
# ============================================================

def effort_summary(
    team_name_to_owner: dict,    # {"Vinicius Chiaradia": 81782664, ...}
    date_from: str,
    date_to: str,
    force_refresh: bool = False,
) -> dict:
    """Pulls calls + emails + meetings dos 11 SDRs e agrega tudo.

    Retorna dict com mesmo shape do _mock_sdr_effort do app.py:
      per_sdr, totals, per_contact, series.
    """
    owner_to_name = {v: k for k, v in team_name_to_owner.items()}
    owner_ids = list(team_name_to_owner.values())

    # 1) Pull dos 3 buckets
    df_calls    = calls_for_owners(owner_ids, date_from, date_to, force_refresh)
    df_emails   = emails_for_owners(owner_ids, date_from, date_to, force_refresh)
    df_meetings = meetings_for_owners(owner_ids, date_from, date_to, force_refresh)

    # 2) Concat — todos têm coluna 'bucket' já preenchida
    parts = [df for df in (df_calls, df_emails, df_meetings) if len(df)]
    if not parts:
        return {
            "per_sdr": [{"sdr": n, "calls": 0, "emails": 0, "meetings": 0,
                         "total": 0, "contacts": 0, "tasks_per_contact": 0,
                         "is_new": False} for n in team_name_to_owner],
            "totals": {"calls": 0, "emails": 0, "meetings": 0, "tasks": 0,
                       "contacts": 0, "active_sdrs": 0},
            "per_contact": [],
            "series": [],
        }
    df_all = pd.concat(parts, ignore_index=True)

    # 3) Associações engagement → contato (3 batches separados, depois concat)
    assoc_parts = []
    for bucket in ("calls", "emails", "meetings"):
        sub = df_all[df_all["bucket"] == bucket]
        if not len(sub):
            continue
        a = engagement_contact_associations(
            ENGAGEMENT_CONFIG[bucket]["object_type"],
            sub["engagement_id"].tolist(),
            force_refresh,
        )
        if len(a):
            assoc_parts.append(a)
    df_assoc = (pd.concat(assoc_parts, ignore_index=True) if assoc_parts
                else pd.DataFrame(columns=["engagement_id", "contact_id"]))

    # 4) Per SDR
    contacts_per_sdr = (df_all.merge(df_assoc, on="engagement_id", how="left")
                              .groupby("owner_id")["contact_id"].nunique()
                              .to_dict())

    per_sdr = []
    for owner_id, name in owner_to_name.items():
        sub = df_all[df_all["owner_id"] == owner_id]
        calls    = int((sub["bucket"] == "calls").sum())
        emails   = int((sub["bucket"] == "emails").sum())
        meetings = int((sub["bucket"] == "meetings").sum())
        total    = calls + emails + meetings
        contacts = int(contacts_per_sdr.get(owner_id, 0))
        per_sdr.append({
            "sdr":      name,
            "calls":    calls,
            "emails":   emails,
            "meetings": meetings,
            "total":    total,
            "contacts": contacts,
            "tasks_per_contact": round(total / contacts, 1) if contacts else 0,
            "is_new":   False,
        })
    per_sdr.sort(key=lambda r: -r["total"])

    totals = {
        "calls":       sum(s["calls"]    for s in per_sdr),
        "emails":      sum(s["emails"]   for s in per_sdr),
        "meetings":    sum(s["meetings"] for s in per_sdr),
        "tasks":       sum(s["total"]    for s in per_sdr),
        "contacts":    int(df_assoc["contact_id"].nunique()) if len(df_assoc) else 0,
        "active_sdrs": sum(1 for s in per_sdr if s["total"] > 0),
    }

    # 5) Per contato (top 50)
    df_full = df_all.merge(df_assoc, on="engagement_id", how="inner")
    if len(df_full):
        df_contacts = contacts_by_id(df_full["contact_id"].unique().tolist(),
                                      force_refresh)
        contact_lookup = df_contacts.set_index("contact_id").to_dict("index")

        agg = (df_full.groupby(["contact_id", "owner_id"])
                       .agg(calls=("bucket", lambda x: (x == "calls").sum()),
                            emails=("bucket", lambda x: (x == "emails").sum()),
                            meetings=("bucket", lambda x: (x == "meetings").sum()),
                            first_task=("date", "min"),
                            last_task=("date", "max"))
                       .reset_index())
        agg["total"] = agg["calls"] + agg["emails"] + agg["meetings"]
        agg = agg.sort_values("total", ascending=False).head(50)

        per_contact = []
        for _, r in agg.iterrows():
            c_info = contact_lookup.get(str(r["contact_id"]), {})
            per_contact.append({
                "contact":    c_info.get("name") or "(sem nome)",
                "company":    c_info.get("company") or "—",
                "sdr":        owner_to_name.get(int(r["owner_id"]), str(r["owner_id"])),
                "calls":      int(r["calls"]),
                "emails":     int(r["emails"]),
                "meetings":   int(r["meetings"]),
                "total":      int(r["total"]),
                "first_task": r["first_task"].strftime("%d %b %Y").lower()
                                if pd.notna(r["first_task"]) else "—",
                "last_task":  r["last_task"].strftime("%d %b %Y").lower()
                                if pd.notna(r["last_task"]) else "—",
            })
    else:
        per_contact = []

    # 6) Série diária (template/route depois agrega pelo grain)
    df_all["d"] = df_all["date"].dt.date
    daily = (df_all.groupby(["d", "bucket"]).size()
                   .unstack(fill_value=0)
                   .reset_index())
    for col in ("calls", "emails", "meetings"):
        if col not in daily.columns:
            daily[col] = 0
    daily["date"] = daily["d"]
    series = daily[["date", "calls", "emails", "meetings"]].to_dict("records")

    return {
        "per_sdr":     per_sdr,
        "totals":      totals,
        "per_contact": per_contact,
        "series":      series,
    }


# ============================================================
# WhatsApp returns nas NOTES (integrador) → última resposta de contato por deal
# ============================================================
# O integrador de WhatsApp grava a conversa como NOTE. wa_notes.py detecta se
# houve retorno do cliente (remetente ≠ vendedor/dono da nota). Aqui a gente
# resolve, para um conjunto de deals, a data do ÚLTIMO retorno de contato.
# Só roda com token (is_configured()); o app trata a ausência como no-op.

import wa_notes  # parser local, sem dependências externas


def all_owners(force_refresh: bool = False) -> dict:
    """Mapa {owner_id(str): nome} de todos os owners do portal. Cacheado (1 dia).
    Usado pra saber o nome do VENDEDOR dono de cada nota (base do parser)."""
    cache_key = "owners_all"
    if not force_refresh:
        cached = _cache_read(cache_key, ttl=86400)
        if cached is not None:
            return dict(zip(cached["owner_id"].astype(str), cached["name"]))
    rows, after = [], None
    while True:
        path = "/crm/v3/owners?limit=100" + (f"&after={after}" if after else "")
        r = requests.get(f"{API}{path}", headers=_headers(), timeout=30)
        if r.status_code >= 400:
            raise HubSpotError(f"HubSpot {r.status_code} em {path}: {r.text[:200]}")
        j = r.json()
        for o in j.get("results", []):
            full = f"{o.get('firstName','') or ''} {o.get('lastName','') or ''}".strip()
            rows.append({"owner_id": str(o.get("id")), "name": full or (o.get("email") or "")})
        after = j.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
    df = pd.DataFrame(rows)
    _cache_write(cache_key, df)
    return dict(zip(df["owner_id"], df["name"]))


def _deal_note_ids(deal_ids: list[str]) -> dict:
    """v4 batch: {deal_id: [note_id, ...]} — quais notas cada deal tem."""
    out: dict[str, list[str]] = {}
    ids = [str(d) for d in deal_ids]
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        body = {"inputs": [{"id": d} for d in chunk]}
        resp = _post("/crm/v4/associations/deals/notes/batch/read", body)
        for item in resp.get("results", []):
            frm = str(item.get("from", {}).get("id"))
            tos = [str(t.get("toObjectId")) for t in item.get("to", [])]
            if tos:
                out.setdefault(frm, []).extend(tos)
    return out


def last_client_returns(deal_ids: list[str], lookback_days: int = 45,
                        force_refresh: bool = False) -> dict:
    """Para os deals dados, devolve {deal_id: iso_ts} da ÚLTIMA nota de WhatsApp
    com retorno de um contato (remetente ≠ vendedor). Deals sem retorno não
    entram no dict. Cacheado por (hash dos deals, lookback); TTL 6h — pull caro.

    Estratégia (limitada ao nosso universo de ~1k deals):
      1) v4 batch deals→notes  → ids de nota por deal
      2) batch-read dos corpos (hs_note_body + ts + owner)
      3) filtra janela + is_whatsapp + has_client_return(owner=vendedor)
      4) max(ts) por deal
    """
    if not deal_ids:
        return {}
    key = f"wa_returns_{lookback_days}_{hash(tuple(sorted(str(d) for d in deal_ids))) & 0xFFFFFFFF}"
    if not force_refresh:
        cached = _cache_read(key, ttl=21600)
        if cached is not None and len(cached):
            return dict(zip(cached["deal_id"].astype(str), cached["last_return"]))
        if cached is not None:  # cache vazio válido
            return {}

    owners = all_owners()
    deal_notes = _deal_note_ids(deal_ids)
    note_to_deals: dict[str, list[str]] = {}
    for did, nids in deal_notes.items():
        for nid in nids:
            note_to_deals.setdefault(nid, []).append(did)
    all_note_ids = list(note_to_deals.keys())

    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=lookback_days)
    last: dict[str, pd.Timestamp] = {}
    for i in range(0, len(all_note_ids), 100):
        chunk = all_note_ids[i:i + 100]
        body = {"inputs": [{"id": nid} for nid in chunk],
                "properties": ["hs_note_body", "hs_timestamp", "hubspot_owner_id"]}
        resp = _post("/crm/v3/objects/notes/batch/read", body)
        for r in resp.get("results", []):
            p = r.get("properties", {})
            ts = pd.to_datetime(p.get("hs_timestamp"), errors="coerce", utc=True)
            if pd.isna(ts) or ts < cutoff:
                continue
            ae = owners.get(str(p.get("hubspot_owner_id")), "")
            if not wa_notes.has_client_return(p.get("hs_note_body", ""), ae):
                continue
            for did in note_to_deals.get(str(r.get("id")), []):
                if did not in last or ts > last[did]:
                    last[did] = ts

    df = pd.DataFrame(
        [{"deal_id": d, "last_return": t.isoformat()} for d, t in last.items()],
        columns=["deal_id", "last_return"])
    _cache_write(key, df)
    return dict(zip(df["deal_id"], df["last_return"]))
