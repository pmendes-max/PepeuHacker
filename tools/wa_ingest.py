#!/usr/bin/env python3
"""Carga dos retornos de cliente via WhatsApp (notas do Sniper no HubSpot) no BigQuery.

Por que existe: o app no Cloud Run não tem token do HubSpot (perdido em 08/2026),
e o BigQuery não recebe notas desde 27/03/2026 (`hubspot.hubspot_notes` congelada).
O que lê o HubSpot é o CONECTOR do Claude — então a coleta roda numa sessão do
Claude (rotina diária "Inatividade · WhatsApp"), e este script faz o resto: parse,
classificação, gravação e o plano das consultas de associação nota → deal.

Tabelas (dataset `tractian-bi.pepeuhacker`, criadas sob demanda):
  wa_notes          1 linha por nota (MERGE por note_id; a nota cresce durante o dia)
  wa_deal_returns   (deal_id, return_date): o deal teve retorno de cliente no dia UTC
  wa_mapped         (return_date, note_id): o que já foi associado a deal
  wa_runs           log de cada execução

Fluxo (o mesmo na carga histórica e na diária):
  1. plan                        → janelas de dia (UTC) a buscar no HubSpot
  2. [conector] search NOTE por janela, páginas de 200 → arquivos JSON salvos
  3. notes ARQ...                → parse + classificação + MERGE em wa_notes
  4. groups --out G.json         → lotes de ≤500 notas por dia com retorno
  5. [conector] search DEAL associatedWith notes IN (lote) → arquivos/IDs
  6. deals --groups G.json --group N ARQ...  → wa_deal_returns + wa_mapped
  7. status                      → frescor e contagens

Datas: tudo em UTC, igual às outras fontes da inatividade (bq._activity_cte).
O retorno é guardado no grão de DIA: a consulta de associação devolve os deals de
um lote de notas sem dizer qual nota é de qual deal, e a inatividade é contada em
dias — o grão não perde nada que a tela use.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
import warnings
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone

warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import wa_notes  # noqa: E402

PROJECT = "tractian-bi"
DS = f"{PROJECT}.pepeuhacker"
T_NOTES, T_DEALS, T_MAPPED, T_RUNS = (f"{DS}.wa_notes", f"{DS}.wa_deal_returns",
                                       f"{DS}.wa_mapped", f"{DS}.wa_runs")
SNIPER_START = date(2026, 5, 14)       # 1ª nota do Sniper no portal

_client = None


def bq():
    global _client
    if _client is None:
        from google.cloud import bigquery
        _client = bigquery.Client(project=PROJECT)
    return _client


DDL = [
    f"""CREATE TABLE IF NOT EXISTS `{T_NOTES}` (
      note_id STRING NOT NULL, owner_id STRING, owner_name STRING, label STRING,
      created_at TIMESTAMP, last_msg_at TIMESTAMP, modified_at TIMESTAMP,
      senders ARRAY<STRUCT<name STRING, key STRING, n INT64, first_at TIMESTAMP,
                           last_at TIMESTAMP, label_match BOOL, dates ARRAY<DATE>,
                           role STRING>>,
      has_client_return BOOL, client_dates ARRAY<DATE>, last_client_at TIMESTAMP,
      n_client_msgs INT64, n_vendor_msgs INT64, n_lines INT64,
      ts_check_minutes INT64, parser_version STRING, ingested_at TIMESTAMP)
    PARTITION BY DATE(created_at) CLUSTER BY owner_id""",
    f"""CREATE TABLE IF NOT EXISTS `{T_DEALS}` (
      deal_id STRING NOT NULL, return_date DATE NOT NULL, mapped_at TIMESTAMP,
      run_id STRING, grain STRING) CLUSTER BY deal_id""",
    f"ALTER TABLE `{T_DEALS}` ADD COLUMN IF NOT EXISTS grain STRING",
    f"ALTER TABLE `{T_DEALS}` ADD COLUMN IF NOT EXISTS match STRING",
    f"""CREATE TABLE IF NOT EXISTS `{T_MAPPED}` (
      return_date DATE NOT NULL, note_id STRING NOT NULL, mapped_at TIMESTAMP,
      run_id STRING)""",
    f"""CREATE TABLE IF NOT EXISTS `{T_RUNS}` (
      run_id STRING, run_at TIMESTAMP, kind STRING, detail STRING)""",
]


def dml(sql: str, job_config=None):
    """Executa DML com retry. A carga histórica roda vários agentes em paralelo
    gravando nas mesmas tabelas, e o BigQuery recusa MERGE simultâneo na mesma
    tabela com "Could not serialize access ... due to concurrent update"."""
    import random
    import time
    for i in range(10):
        try:
            return bq().query(sql, job_config=job_config).result()
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            if i < 9 and ("concurrent update" in msg or "serialize access" in msg
                          or "rateLimitExceeded" in msg or "Exceeded rate limits" in msg):
                time.sleep(min(60, 2 ** i) + random.random() * 3)
                continue
            raise


def ensure_tables():
    for q in DDL:
        dml(q)


def log_run(kind: str, detail: dict, run_id: str | None = None):
    from google.cloud import bigquery
    rid = run_id or os.environ.get("WA_RUN_ID") or uuid.uuid4().hex[:12]
    dml(f"INSERT `{T_RUNS}` (run_id, run_at, kind, detail) VALUES (@r, CURRENT_TIMESTAMP(), @k, @d)",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("r", "STRING", rid),
            bigquery.ScalarQueryParameter("k", "STRING", kind),
            bigquery.ScalarQueryParameter("d", "STRING", json.dumps(detail, default=str))]))
    return rid


# ---------------------------------------------------------------- leitura dos arquivos

def read_results(path: str) -> tuple[list, dict]:
    """Arquivo salvo de um search_crm_objects → (results, meta {total, offset})."""
    raw = open(path, encoding="utf-8").read().strip()
    j = json.loads(raw)
    if isinstance(j, list):                      # [{type:'text', text:'{...}'}]
        j = j[0]
    if isinstance(j, dict) and "text" in j and "results" not in j:
        j = json.loads(j["text"])
    return j.get("results", []), {"total": j.get("total"), "offset": j.get("offset")}


def owners() -> dict:
    df = bq().query(f"""
      SELECT CAST(ownerId AS STRING) id, TRIM(CONCAT(IFNULL(firstName,''),' ',IFNULL(lastName,''))) name
      FROM `{PROJECT}.hubspot.hubspot_owner_id`
      UNION ALL
      SELECT CAST(ownerId AS STRING), TRIM(CONCAT(IFNULL(firstName,''),' ',IFNULL(lastName,'')))
      FROM `{PROJECT}.hubspot.hubspot_owner_id_archived`""").to_dataframe()
    return dict(zip(df["id"], df["name"]))


# ---------------------------------------------------------------- apelidos do vendedor

def learn_aliases(items) -> dict:
    """{owner_id: set(chaves de remetente)} = como cada owner aparece no WhatsApp.

    `items`: iterável de (owner_id, label_key, [(sender_key, label_match), ...]).
    Candidato = remetente que NÃO é o rótulo. Vira apelido quando aparece em ≥3
    conversas (rótulos) diferentes do mesmo owner — o colega do cliente aparece
    numa conversa só; o vendedor, em todas. Nota de remetente único sem fala do
    rótulo (o vendedor puxando conversa) conta com o mesmo peso."""
    seen = defaultdict(lambda: defaultdict(set))
    for owner_id, label_key, snds in items:
        if not owner_id:
            continue
        for k, lm in snds:
            if not lm:
                seen[owner_id][k].add(label_key)
    return {o: {k for k, labs in d.items() if len(labs) >= 3} for o, d in seen.items()}


def history_alias_items():
    """(owner_id, label_key, [(key, label_match)]) de tudo que já está em wa_notes."""
    try:
        df = bq().query(f"""
          SELECT owner_id, label, ARRAY(SELECT AS STRUCT s.key, s.label_match FROM UNNEST(senders) s) snd
          FROM `{T_NOTES}`""").to_dataframe()
    except Exception:
        return []
    out = []
    for o, lab, snd in zip(df["owner_id"], df["label"], df["snd"]):
        out.append((o, wa_notes._norm(lab or ""), [(x["key"], bool(x["label_match"])) for x in snd]))
    return out


INTERNAL_NAMES: list = []   # nomes completos dos owners (preenchido por set_internal)


def set_internal(owner_map: dict):
    global INTERNAL_NAMES
    INTERNAL_NAMES = sorted({wa_notes._norm(n) for n in owner_map.values()
                             if n and len(wa_notes._toks(wa_notes._norm(n))) >= 2})


def staff_keys(owner_map: dict, aliases: dict) -> set:
    ks = {wa_notes._norm(n) for n in owner_map.values() if n}
    for a in aliases.values():
        ks |= a
    return {k for k in ks if k}


# ---------------------------------------------------------------- linhas de wa_notes

def build_row(note_id, p_raw: dict, parsed, owner_map, aliases, staff, now):
    owner_id = p_raw.get("hubspot_owner_id") or None
    oname = owner_map.get(owner_id or "", "")
    own_alias = aliases.get(owner_id or "", set())
    # O apelido de OUTRO owner não pode virar "staff" pra quem é o próprio dono.
    roles = wa_notes.classify(parsed, oname, own_alias, staff - own_alias,
                              internal_names=INTERNAL_NAMES)
    summ = wa_notes.client_summary(parsed, roles)
    last_msg = wa_notes._to_dt(p_raw.get("hs_timestamp"))
    rec_last = max((s.last_at for s in parsed.senders.values() if s.last_at), default=None)
    chk = int(abs((rec_last - last_msg).total_seconds()) // 60) if (rec_last and last_msg) else None
    iso = lambda d: d.isoformat() if d else None
    return {
        "note_id": str(note_id), "owner_id": owner_id, "owner_name": oname or None,
        "label": parsed.label,
        "created_at": iso(wa_notes._to_dt(p_raw.get("hs_createdate"))),
        "last_msg_at": iso(last_msg),
        "modified_at": iso(wa_notes._to_dt(p_raw.get("hs_lastmodifieddate"))),
        "senders": [{"name": s.name, "key": k, "n": s.n, "first_at": iso(s.first_at),
                     "last_at": iso(s.last_at), "label_match": s.label_match,
                     "dates": sorted(d.isoformat() for d in s.dates), "role": roles.get(k)}
                    for k, s in parsed.senders.items()],
        "has_client_return": summ["has_client_return"],
        "client_dates": [d.isoformat() for d in summ["client_dates"]],
        "last_client_at": iso(summ["last_client_at"]),
        "n_client_msgs": summ["n_client_msgs"], "n_vendor_msgs": summ["n_vendor_msgs"],
        "n_lines": parsed.n_lines, "ts_check_minutes": chk,
        "parser_version": wa_notes.PARSER_VERSION, "ingested_at": now.isoformat(),
    }


def _clean(v):
    """Texto seguro pro load JSON do BQ: sem NUL/controle e sem surrogate solto
    (emoji partido ao meio no nome do contato derrubava o load inteiro)."""
    if isinstance(v, str):
        v = v.encode("utf-8", "replace").decode("utf-8")
        # U+2028/2029/0085 saem crus no JSON (ensure_ascii=False) e o leitor de
        # NEWLINE_DELIMITED_JSON do BQ os trata como quebra de linha.
        v = re.sub(r"[\u2028\u2029\u0085\r\n\t]", " ", v)
        return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", v)
    if isinstance(v, list):
        return [_clean(x) for x in v]
    if isinstance(v, dict):
        return {k: _clean(x) for k, x in v.items()}
    if isinstance(v, float) and v != v:          # NaN (owner_id nulo vindo do pandas)
        return None
    return v


def merge_notes(rows: list):
    """MERGE em wa_notes: entra a versão mais nova (modified_at) de cada nota."""
    from google.cloud import bigquery
    if not rows:
        return 0
    rows = [_clean(r) for r in rows]
    tmp = f"{DS}._wa_notes_stage_{uuid.uuid4().hex[:8]}"
    schema = bq().get_table(T_NOTES).schema
    cfg = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE",
                                 source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
    bq().load_table_from_json(rows, tmp, job_config=cfg).result()
    try:
        cols = [f.name for f in schema]
        dml(f"""
          MERGE `{T_NOTES}` t
          USING (SELECT * FROM `{tmp}` QUALIFY ROW_NUMBER() OVER
                 (PARTITION BY note_id ORDER BY modified_at DESC, ingested_at DESC) = 1) s
          ON t.note_id = s.note_id
          WHEN MATCHED AND (s.modified_at > t.modified_at OR s.parser_version != t.parser_version
                            OR t.modified_at IS NULL) THEN
            UPDATE SET {", ".join(f"{c} = s.{c}" for c in cols if c != "note_id")}
          WHEN NOT MATCHED THEN INSERT ({", ".join(cols)}) VALUES ({", ".join("s." + c for c in cols)})
        """)
    finally:
        bq().delete_table(tmp, not_found_ok=True)
    return len(rows)


# ---------------------------------------------------------------- comandos

def cmd_notes(a):
    ensure_tables()
    now = datetime.now(timezone.utc)
    raw = {}
    pages = []
    for f in a.files:
        res, meta = read_results(f)
        pages.append({"file": os.path.basename(f), "n": len(res), **meta})
        for r in res:
            p = r.get("properties", {})
            nid = str(r.get("id") or p.get("hs_object_id"))
            old = raw.get(nid)
            if old is None or (p.get("hs_lastmodifieddate") or "") > (old.get("hs_lastmodifieddate") or ""):
                raw[nid] = p
    owner_map = owners()
    set_internal(owner_map)
    parsed = {}
    skipped = Counter()
    for nid, p in raw.items():
        body = p.get("hs_note_body")
        if body is None:
            skipped["sem_corpo"] += 1
            continue
        pn = wa_notes.parse(body, p.get("hs_createdate"), p.get("hs_timestamp"))
        if pn is None:
            skipped["sem_cabecalho"] += 1
            continue
        parsed[nid] = pn
    items = history_alias_items() + [
        (raw[n].get("hubspot_owner_id"), pn.label_key,
         [(k, s.label_match) for k, s in pn.senders.items()]) for n, pn in parsed.items()]
    aliases = learn_aliases(items)
    staff = staff_keys(owner_map, aliases)
    rows = [build_row(n, raw[n], pn, owner_map, aliases, staff, now) for n, pn in parsed.items()]
    merge_notes(rows)
    chk = [r["ts_check_minutes"] for r in rows if r["ts_check_minutes"] is not None]
    out = {"pages": len(pages), "notes_in_files": len(raw), "parsed": len(rows),
           "skipped": dict(skipped),
           "with_client_return": sum(r["has_client_return"] for r in rows),
           "ts_check_ok_pct": round(100 * sum(c <= 2 for c in chk) / len(chk), 1) if chk else None,
           "page_meta": pages}
    log_run("notes", out)
    print(json.dumps(out, ensure_ascii=False, indent=1))


def _pydt(v):
    """Timestamp vindo do BQ (pandas.Timestamp no topo, datetime dentro de
    STRUCT, NaT/None quando nulo) → datetime com tz, ou None."""
    if v is None:
        return None
    try:
        import pandas as pd
        if pd.isna(v):
            return None
        if isinstance(v, pd.Timestamp):
            v = v.to_pydatetime()
    except (TypeError, ValueError):
        pass
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    return wa_notes._to_dt(v)


def cmd_reset_mapping(a):
    """Apaga a associação nota→deal (wa_deal_returns + wa_mapped) pra refazê-la
    do zero depois de uma mudança no classificador. As notas ficam."""
    ensure_tables()
    dml(f"TRUNCATE TABLE `{T_DEALS}`")
    dml(f"TRUNCATE TABLE `{T_MAPPED}`")
    log_run("reset_mapping", {"reason": a.reason})
    print(json.dumps({"ok": True}))


def cmd_reclassify(a):
    """Reaplica a classificação em TUDO que está em wa_notes com os apelidos
    aprendidos do histórico inteiro — sem buscar nada no HubSpot."""
    ensure_tables()
    now = datetime.now(timezone.utc)
    df = bq().query(f"SELECT * FROM `{T_NOTES}`").to_dataframe()
    owner_map = owners()
    set_internal(owner_map)
    items = [(o, wa_notes._norm(l or ""), [(s["key"], bool(s["label_match"])) for s in snd])
             for o, l, snd in zip(df["owner_id"], df["label"], df["senders"])]
    aliases = learn_aliases(items)
    staff = staff_keys(owner_map, aliases)
    rows, changed = [], 0
    for r in df.to_dict("records"):
        pn = wa_notes.ParsedNote(label=r["label"], label_key=wa_notes._norm(r["label"] or ""),
                                 senders={}, n_lines=int(r["n_lines"] or 0))
        for s in r["senders"]:
            pn.senders[s["key"]] = wa_notes.Sender(
                name=s["name"], key=s["key"], n=int(s["n"]),
                first_at=_pydt(s["first_at"]), last_at=_pydt(s["last_at"]),
                label_match=bool(s["label_match"]),
                dates={d if isinstance(d, date) else date.fromisoformat(str(d)[:10]) for d in s["dates"]})
        iso = lambda v: (_pydt(v).isoformat() if _pydt(v) is not None else None)
        oid = r["owner_id"] if isinstance(r["owner_id"], str) and r["owner_id"] else None
        p_raw = {"hubspot_owner_id": oid, "hs_createdate": iso(r["created_at"]),
                 "hs_timestamp": iso(r["last_msg_at"]), "hs_lastmodifieddate": iso(r["modified_at"])}
        new = build_row(r["note_id"], p_raw, pn, owner_map, aliases, staff, now)
        new["parser_version"] = wa_notes.PARSER_VERSION + "+reclass"
        if bool(new["has_client_return"]) != bool(r["has_client_return"]):
            changed += 1
        rows.append(new)
    # Reescreve via MERGE (parser_version muda → UPDATE).
    for i in range(0, len(rows), 5000):
        merge_notes(rows[i:i + 5000])
    out = {"notes": len(rows), "return_flag_changed": changed,
           "owners_with_aliases": sum(1 for v in aliases.values() if v)}
    log_run("reclassify", out)
    print(json.dumps(out, ensure_ascii=False))


def cmd_plan(a):
    """Janelas de dia UTC pra buscar. Diário: do dia do último modified_at já
    gravado (com 1 dia de folga, a nota cresce) até hoje. Histórico: --from/--to.
    Dias já carregados por inteiro saem da lista com --skip-done."""
    ensure_tables()
    today = datetime.now(timezone.utc).date()
    if a.date_from:
        lo = date.fromisoformat(a.date_from)
    else:
        r = list(bq().query(f"SELECT DATE(MAX(modified_at)) d FROM `{T_NOTES}`").result())[0]
        lo = (r["d"] - timedelta(days=1)) if r["d"] else SNIPER_START
    hi = date.fromisoformat(a.date_to) if a.date_to else today
    days = [lo + timedelta(days=i) for i in range((hi - lo).days + 1)]
    done = set()
    if a.skip_done:
        q = list(bq().query(f"""SELECT DISTINCT JSON_VALUE(detail, '$.day') d FROM `{T_RUNS}`
                                WHERE kind = 'day_done'""").result())
        done = {x["d"] for x in q if x["d"]}
    # Hoje nunca é marcado como feito (o dia ainda está acontecendo).
    days = [d for d in days if d.isoformat() not in done]
    if a.newest_first:
        days = sorted(days, reverse=True)
    out = [{"day": d.isoformat(),
            "filter": {"propertyName": a.prop, "operator": "BETWEEN",
                       "value": f"{d.isoformat()}T00:00:00Z", "highValue": f"{d.isoformat()}T23:59:59.999Z"}}
           for d in days]
    print(json.dumps({"by": a.prop, "days": out}, ensure_ascii=False, indent=1))


def cmd_day_done(a):
    """Marca dias como carregados por inteiro (todas as páginas de notas da
    janela foram salvas e gravadas). Nunca marque o dia de hoje."""
    ensure_tables()
    today = datetime.now(timezone.utc).date().isoformat()
    days = [d for d in a.days if d < today]
    for d in days:
        log_run("day_done", {"day": d, "notes": a.notes})
    print(json.dumps({"ok": True, "days": days, "ignored_today_or_future": sorted(set(a.days) - set(days))}))


DAY_GRAIN_DAYS = 42   # últimos 42 dias por DIA; antes disso, por SEMANA


def cmd_groups(a):
    """Lotes de notas com retorno de cliente ainda não associadas a deal.
    Cada lote = 1 consulta DEAL com até 5 filterGroups × 100 notas (OU).

    Grão: dia nos últimos DAY_GRAIN_DAYS dias; antes disso, semana (seg–dom),
    com a data do ÚLTIMO dia da semana. Retorno com mais de 42 dias já estoura
    qualquer SLA (o maior é 21d) mesmo adiantado em 6 dias — e no snapshot de
    domingo do WoW (até 14 dias atrás) também. Corta ~70% das consultas da
    carga histórica sem mudar o status de nenhum deal."""
    ensure_tables()
    where = []
    if a.date_from:
        where.append(f"d >= DATE '{a.date_from}'")
    if a.date_to:
        where.append(f"d <= DATE '{a.date_to}'")
    cut = (datetime.now(timezone.utc).date() - timedelta(days=DAY_GRAIN_DAYS)).isoformat()
    df = bq().query(f"""
      WITH c AS (
        SELECT n.note_id, d FROM `{T_NOTES}` n, UNNEST(n.client_dates) d
        WHERE n.has_client_return)
      SELECT c.d, c.note_id,
             IF(c.d >= DATE '{cut}', c.d,
                LEAST(DATE_ADD(DATE_TRUNC(c.d, WEEK(MONDAY)), INTERVAL 6 DAY),
                      DATE_SUB(DATE '{cut}', INTERVAL 1 DAY))) AS bucket,
             c.d >= DATE '{cut}' AS is_day
      FROM c
      LEFT JOIN `{T_MAPPED}` m ON m.return_date = c.d AND m.note_id = c.note_id
      WHERE m.note_id IS NULL {"AND " + " AND ".join(where) if where else ""}
      ORDER BY bucket DESC, c.note_id""").to_dataframe()
    groups = []
    for (b, is_day), sub in df.groupby(["bucket", "is_day"], sort=False):
        items = [[str(n), str(d)] for n, d in zip(sub["note_id"], sub["d"])]
        ids = list(dict.fromkeys(str(n) for n in sub["note_id"]))
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            cs = set(chunk)
            fg = [{"associatedWith": [{"objectType": "notes", "operator": "IN",
                                       "objectIdValues": [int(x) for x in chunk[j:j + 100]]}]}
                  for j in range(0, len(chunk), 100)]
            groups.append({"group": len(groups), "date": str(b),
                           "grain": "day" if is_day else "week",
                           "note_ids": chunk, "items": [it for it in items if it[0] in cs],
                           "filterGroups": fg})
    with open(a.out, "w") as fh:
        json.dump({"created": datetime.now(timezone.utc).isoformat(), "groups": groups}, fh)
    print(json.dumps({"groups": len(groups), "notes": int(df["note_id"].nunique()) if len(df) else 0,
                      "day_groups": sum(g["grain"] == "day" for g in groups),
                      "week_groups": sum(g["grain"] == "week" for g in groups), "file": a.out}))


OWNER_DAYS = 42   # janela da associação por dono (SLA máx. 21d + WoW 14d + folga)


def open_deal_owners() -> set:
    """Owners com deal ABERTO nos funis Sales BR / Sales MX — os únicos cuja
    conversa pode mudar o status de algum deal na Inatividade."""
    df = bq().query(f"""
      SELECT DISTINCT CAST(account_executive_id AS STRING) o
      FROM `{PROJECT}.dbt_analytics_dev.int_adding_names_to_deals`
      WHERE pipeline_label IN ('Sales BR', 'Sales MX') AND account_executive_id IS NOT NULL
        AND NOT REGEXP_CONTAINS(LOWER(stage_label), r'won|lost|no show')""").to_dataframe()
    return set(df["o"])


def cmd_groups_owner(a):
    """Lotes da associação POR DONO: o WhatsApp só conta pro deal quando a
    conversa é do DONO do deal.

    Por que (08/10/2026): o HubSpot pendura a nota do Sniper em TODOS os deals
    da empresa do contato. Sem o dono, a conversa do Gustavo Maltez com um
    contato da Vale virava "retorno" no deal do Lucas Allgayer (Vale - Mina
    Onça Puma), e em conta grande qualquer conversa de qualquer pessoa da
    Tractian zerava a inatividade de todos os deals.

    Cada lote = 1 busca DEAL com até 5 filterGroups; cada filterGroup = notas
    de UM owner num dia (≤100) E hubspot_owner_id do deal = esse owner. Como o
    filtro de dono está no MESMO filterGroup da associação, todo deal devolvido
    é do owner cujas notas o trouxeram — a associação sai exata, sem precisar
    saber qual nota é de qual deal. Só dia (grão diário) nos últimos
    OWNER_DAYS dias: retorno mais velho que isso não muda status de SLA."""
    ensure_tables()
    since = (datetime.now(timezone.utc).date() - timedelta(days=a.days)).isoformat()
    owners_ok = open_deal_owners()
    df = bq().query(f"""
      WITH c AS (
        SELECT n.note_id, n.owner_id, d FROM `{T_NOTES}` n, UNNEST(n.client_dates) d
        WHERE n.has_client_return AND n.owner_id IS NOT NULL)
      SELECT c.d, c.owner_id, c.note_id FROM c
      LEFT JOIN `{T_MAPPED}` m ON m.return_date = c.d AND m.note_id = c.note_id
      WHERE m.note_id IS NULL AND c.d >= DATE '{since}'
      ORDER BY c.d DESC, c.owner_id, c.note_id""").to_dataframe()
    skipped = df[~df["owner_id"].isin(owners_ok)]
    df = df[df["owner_id"].isin(owners_ok)]
    groups = []
    for d, sub in df.groupby("d", sort=False):
        chunks = []                      # (owner, [note_ids ≤100])
        for o, so in sub.groupby("owner_id", sort=True):
            ids = list(dict.fromkeys(str(x) for x in so["note_id"]))
            for i in range(0, len(ids), 100):
                chunks.append((str(o), ids[i:i + 100]))
        for i in range(0, len(chunks), 5):
            part = chunks[i:i + 5]
            fg = [{"associatedWith": [{"objectType": "notes", "operator": "IN",
                                       "objectIdValues": [int(x) for x in ids]}],
                   "filters": [{"propertyName": "hubspot_owner_id", "operator": "EQ", "value": o}]}
                  for o, ids in part]
            note_ids = [x for _, ids in part for x in ids]
            groups.append({"group": len(groups), "date": str(d), "grain": "day", "mode": "owner",
                           "owners": [o for o, _ in part], "note_ids": note_ids,
                           "items": [[x, str(d)] for x in note_ids], "filterGroups": fg})
    with open(a.out, "w") as fh:
        json.dump({"created": datetime.now(timezone.utc).isoformat(), "mode": "owner",
                   "since": since, "groups": groups}, fh)
    print(json.dumps({"groups": len(groups), "days": int(df["d"].nunique()) if len(df) else 0,
                      "notes": int(df["note_id"].nunique()) if len(df) else 0,
                      "owner_days": int(df.groupby(["d", "owner_id"]).ngroups) if len(df) else 0,
                      "skipped_notes_owner_sem_deal_aberto": int(skipped["note_id"].nunique()),
                      "since": since, "file": a.out}))


def _ids_from(path: str) -> tuple[list, dict]:
    try:
        res, meta = read_results(path)
        meta["owners"] = {str(r.get("id") or r.get("properties", {}).get("hs_object_id")):
                          (r.get("properties") or {}).get("hubspot_owner_id") for r in res}
        return [str(r.get("id") or r.get("properties", {}).get("hs_object_id")) for r in res], meta
    except (json.JSONDecodeError, KeyError, TypeError, IndexError, AttributeError):
        txt = open(path).read()
        return re.findall(r"\b\d{6,}\b", txt), {}


def cmd_deals(a):
    """Resultado(s) da consulta DEAL de UM lote → wa_deal_returns + wa_mapped.
    Aceita o JSON salvo pelo conector ou um arquivo de texto só com os IDs
    (quando o resultado veio inline). Confere a paginação pelo `total`."""
    from google.cloud import bigquery
    ensure_tables()
    spec = json.load(open(a.groups))["groups"][a.group]
    deal_ids, totals, file_owner = [], [], {}
    for f in a.files:
        ids, meta = _ids_from(f)
        deal_ids += ids
        file_owner.update({k: v for k, v in (meta.get("owners") or {}).items() if v})
        if meta.get("total") is not None:
            totals.append(int(meta["total"]))
    deal_ids = sorted(set(deal_ids))
    if a.expect_total is not None:
        totals.append(a.expect_total)
    if totals and max(totals) != len(deal_ids) and not a.partial_ok:
        print(json.dumps({"ok": False, "erro": "contagem não bate com o total da busca "
                          "(faltam páginas ou ID transcrito errado)", "total": max(totals),
                          "lidos": len(deal_ids)}))
        sys.exit(2)
    # Todo ID tem de existir como deal no BQ: pega ID transcrito errado quando o
    # resultado veio inline e foi copiado pra um arquivo de texto.
    known = {r["d"] for r in bq().query(
        f"SELECT CAST(dealId AS STRING) d FROM `{DS}.hubspot_deals_live` WHERE CAST(dealId AS STRING) IN UNNEST(@ids)",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ArrayQueryParameter("ids", "STRING", deal_ids)])).result()}
    unknown = [d for d in deal_ids if d not in known]
    if unknown and not a.partial_ok:
        print(json.dumps({"ok": False, "erro": "IDs que não existem como deal no BQ",
                          "unknown": unknown[:20], "n_unknown": len(unknown)}))
        sys.exit(3)
    owner_warn = []
    if spec.get("mode") == "owner":
        # O dono de cada deal tem de ser um dos owners do lote: pega ID
        # transcrito errado que por azar é um deal real de outra pessoa.
        allowed = set(spec["owners"])
        bq_owner = {r["d"]: r["o"] for r in bq().query(
            f"""SELECT CAST(deal_id AS STRING) d, CAST(ANY_VALUE(account_executive_id) AS STRING) o
                FROM `{PROJECT}.dbt_analytics_dev.int_adding_names_to_deals`
                WHERE CAST(deal_id AS STRING) IN UNNEST(@ids) GROUP BY 1""",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ArrayQueryParameter("ids", "STRING", deal_ids)])).result()}
        hard = [d for d in deal_ids if d in file_owner and file_owner[d] not in allowed]
        if hard and not a.partial_ok:
            print(json.dumps({"ok": False, "erro": "deal com dono fora do lote (no próprio resultado do HubSpot)",
                              "deals": hard[:20]}))
            sys.exit(4)
        # Sem o owner no arquivo (resultado inline): confere no BQ. A curated
        # atrasa até 2 dias, então dono recém-trocado vira aviso, não erro.
        owner_warn = [d for d in deal_ids if d not in file_owner and bq_owner.get(d)
                      and bq_owner[d] not in allowed]
        if len(owner_warn) > max(2, 0.2 * len(deal_ids)) and not a.partial_ok:
            print(json.dumps({"ok": False, "erro": "muitos deals com dono fora do lote (transcrição?)",
                              "deals": owner_warn[:20]}))
            sys.exit(4)
    rid = os.environ.get("WA_RUN_ID") or uuid.uuid4().hex[:12]
    items = spec.get("items") or [[n, spec["date"]] for n in spec["note_ids"]]
    p = [bigquery.ArrayQueryParameter("deals", "STRING", deal_ids),
         bigquery.ArrayQueryParameter("inotes", "STRING", [str(n) for n, _ in items]),
         bigquery.ArrayQueryParameter("idates", "STRING", [str(d)[:10] for _, d in items]),
         bigquery.ScalarQueryParameter("d", "DATE", spec["date"]),
         bigquery.ScalarQueryParameter("g", "STRING", spec.get("grain", "day")),
         bigquery.ScalarQueryParameter("mt", "STRING", spec.get("mode", "any_note")),
         bigquery.ScalarQueryParameter("r", "STRING", rid)]
    cfg = bigquery.QueryJobConfig(query_parameters=p)
    dml(f"""
      MERGE `{T_DEALS}` t USING (SELECT x AS deal_id FROM UNNEST(@deals) x) s
      ON t.deal_id = s.deal_id AND t.return_date = @d
      WHEN NOT MATCHED THEN INSERT (deal_id, return_date, mapped_at, run_id, grain, match)
        VALUES (s.deal_id, @d, CURRENT_TIMESTAMP(), @r, @g, @mt)""", job_config=cfg)
    dml(f"""
      MERGE `{T_MAPPED}` t USING (
        SELECT n AS note_id, DATE(dd) AS d
        FROM UNNEST(@inotes) n WITH OFFSET i JOIN UNNEST(@idates) dd WITH OFFSET j ON i = j) s
      ON t.note_id = s.note_id AND t.return_date = s.d
      WHEN NOT MATCHED THEN INSERT (return_date, note_id, mapped_at, run_id)
        VALUES (s.d, s.note_id, CURRENT_TIMESTAMP(), @r)""", job_config=cfg)
    out = {"ok": True, "group": a.group, "date": spec["date"], "grain": spec.get("grain", "day"),
           "mode": spec.get("mode", "any_note"), "owner_warn": owner_warn[:10],
           "notes": len(spec["note_ids"]),
           "deals": len(deal_ids)}
    log_run("deals", out, rid)
    print(json.dumps(out))


def cmd_show_group(a):
    """filterGroups (JSON compacto) de um lote, pronto pra colar na busca DEAL."""
    g = json.load(open(a.groups))["groups"][a.group]
    print(json.dumps({"group": a.group, "date": g["date"], "notes": len(g["note_ids"]),
                      "filterGroups": g["filterGroups"]}, separators=(",", ":")))


def cmd_total(a):
    """total / offset / n de um arquivo salvo de search_crm_objects."""
    res, meta = read_results(a.file)
    print(json.dumps({"n": len(res), **meta}))


_STRATA = {
    # nota sem fala do rótulo e com 2+ remetentes (grupo, contato renomeado)
    "no_label_multi": "NOT EXISTS (SELECT 1 FROM UNNEST(senders) s WHERE s.label_match) AND ARRAY_LENGTH(senders) >= 2",
    # cliente reconhecido SEM bater com o rótulo (regra 5)
    "client_not_label": "EXISTS (SELECT 1 FROM UNNEST(senders) s WHERE s.role = 'client' AND NOT s.label_match)",
    # vendedor reconhecido por apelido, com nome diferente do owner
    "vendor_alias": """EXISTS (SELECT 1 FROM UNNEST(senders) s WHERE s.role = 'vendor'
                       AND NOT STARTS_WITH(LOWER(s.key), LOWER(SPLIT(IFNULL(owner_name,''), ' ')[SAFE_OFFSET(0)])))""",
    "mx": "owner_name IN ('Viridiana Garza','Adolfo Mora','Santiago de Oriol','Sabina Diaz','Armando Roman','Luis Terrazas','Adrian Hernandez','Adrián Hernández','Mauricio Del Hierro','Víctor López')",
    "random_return": "has_client_return",
    "random_noreturn": "NOT has_client_return",
    "staff": "EXISTS (SELECT 1 FROM UNNEST(senders) s WHERE s.role = 'staff')",
}


def cmd_sample(a):
    """Amostra de notas de um estrato, com o que o parser concluiu — pra auditar
    contra o corpo real da nota (get_crm_objects)."""
    df = bq().query(f"""
      SELECT note_id, owner_name, label, has_client_return, client_dates,
             ARRAY(SELECT AS STRUCT s.name, s.role, s.label_match, s.n FROM UNNEST(senders) s) senders
      FROM `{T_NOTES}` WHERE {_STRATA[a.stratum]}
      ORDER BY FARM_FINGERPRINT(CONCAT(note_id, '{a.seed}')) LIMIT {int(a.n)}""").to_dataframe()
    out = [{"note_id": r.note_id, "owner": r.owner_name, "label": r.label,
            "has_client_return": bool(r.has_client_return),
            "client_dates": [str(d) for d in r.client_dates],
            "senders": [{"name": s["name"], "role": s["role"], "label_match": bool(s["label_match"]),
                         "n": int(s["n"])} for s in r.senders]} for r in df.itertuples()]
    print(json.dumps(out, ensure_ascii=False))


def cmd_coverage(a):
    """Notas por dia (modified_at, UTC) do início do Sniper até ontem, e os dias
    marcados como carregados. Dia sem nota e sem marca = buraco a recarregar."""
    df = bq().query(f"""
      WITH cal AS (SELECT d FROM UNNEST(GENERATE_DATE_ARRAY(DATE '{SNIPER_START}',
                   DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))) d),
      n AS (SELECT DATE(modified_at) d, COUNT(*) notes FROM `{T_NOTES}` GROUP BY 1),
      done AS (SELECT DISTINCT DATE(JSON_VALUE(detail, '$.day')) d FROM `{T_RUNS}` WHERE kind = 'day_done')
      SELECT cal.d, IFNULL(n.notes, 0) notes, done.d IS NOT NULL is_done
      FROM cal LEFT JOIN n USING (d) LEFT JOIN done USING (d) ORDER BY cal.d""").to_dataframe()
    missing = [str(d) for d, k in zip(df["d"], df["is_done"]) if not k]
    print(json.dumps({"days": len(df), "days_done": int(df["is_done"].sum()),
                      "missing_days": missing, "notes_total": int(df["notes"].sum()),
                      "min_notes_day": int(df["notes"].min()) if len(df) else 0}))


def cmd_status(a):
    ensure_tables()
    q = f"""
      SELECT
        (SELECT COUNT(*) FROM `{T_NOTES}`) notes,
        (SELECT COUNTIF(has_client_return) FROM `{T_NOTES}`) notes_with_return,
        (SELECT MIN(DATE(created_at)) FROM `{T_NOTES}`) first_day,
        (SELECT MAX(modified_at) FROM `{T_NOTES}`) last_note_modified,
        (SELECT COUNT(DISTINCT deal_id) FROM `{T_DEALS}`) deals_with_return,
        (SELECT MAX(return_date) FROM `{T_DEALS}`) last_return_date,
        (SELECT COUNT(*) FROM (SELECT n.note_id, d FROM `{T_NOTES}` n, UNNEST(client_dates) d
                               WHERE has_client_return) c
          LEFT JOIN `{T_MAPPED}` m ON m.note_id = c.note_id AND m.return_date = c.d
          WHERE m.note_id IS NULL) pending_mapping,
        (SELECT MAX(run_at) FROM `{T_RUNS}`) last_run"""
    r = dict(list(bq().query(q).result())[0])
    print(json.dumps(r, default=str, ensure_ascii=False, indent=1))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = ap.add_subparsers(dest="cmd", required=True)
    s = sp.add_parser("plan"); s.add_argument("--from", dest="date_from"); s.add_argument("--to", dest="date_to")
    s.add_argument("--prop", default="hs_lastmodifieddate"); s.add_argument("--skip-done", action="store_true")
    s.add_argument("--newest-first", action="store_true"); s.set_defaults(f=cmd_plan)
    s = sp.add_parser("notes"); s.add_argument("files", nargs="+"); s.set_defaults(f=cmd_notes)
    s = sp.add_parser("day-done"); s.add_argument("days", nargs="+"); s.add_argument("--notes", type=int); s.set_defaults(f=cmd_day_done)
    s = sp.add_parser("show-group"); s.add_argument("groups"); s.add_argument("group", type=int); s.set_defaults(f=cmd_show_group)
    s = sp.add_parser("total"); s.add_argument("file"); s.set_defaults(f=cmd_total)
    s = sp.add_parser("sample"); s.add_argument("--stratum", required=True,
        choices=["no_label_multi", "client_not_label", "vendor_alias", "mx", "random_return", "random_noreturn", "staff"])
    s.add_argument("--n", type=int, default=40); s.add_argument("--seed", default="a"); s.set_defaults(f=cmd_sample)
    s = sp.add_parser("coverage"); s.set_defaults(f=cmd_coverage)
    s = sp.add_parser("groups"); s.add_argument("--out", required=True)
    s.add_argument("--from", dest="date_from"); s.add_argument("--to", dest="date_to"); s.set_defaults(f=cmd_groups)
    s = sp.add_parser("groups-owner"); s.add_argument("--out", required=True)
    s.add_argument("--days", type=int, default=OWNER_DAYS); s.set_defaults(f=cmd_groups_owner)
    s = sp.add_parser("deals"); s.add_argument("--groups", required=True); s.add_argument("--group", type=int, required=True)
    s.add_argument("--partial-ok", action="store_true"); s.add_argument("--expect-total", type=int)
    s.add_argument("files", nargs="+"); s.set_defaults(f=cmd_deals)
    s = sp.add_parser("reclassify"); s.set_defaults(f=cmd_reclassify)
    s = sp.add_parser("reset-mapping"); s.add_argument("--reason", required=True); s.set_defaults(f=cmd_reset_mapping)
    s = sp.add_parser("status"); s.set_defaults(f=cmd_status)
    a = ap.parse_args()
    a.f(a)


if __name__ == "__main__":
    main()
