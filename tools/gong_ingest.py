#!/usr/bin/env python3
"""Carga das reuniões do Gong por contato no BigQuery, pra aba Inatividade.

Por que existe (19/10/2026): o Pedro pediu que reunião feita no Gong conte como
retorno do cliente quando o CONTATO da reunião está associado ao deal do
pipeline de vendas. O Gong grava cada reunião gravada no HubSpot como meeting
("[Gong] Microsoft Teams: ...", origem `hs_object_source_detail_1 = Gong`,
~100 por dia útil), mas:
  • a cópia da nekt no BigQuery (`hubspot_nekt.hubspot_meetings`) parou em
    29/09/2026 17h UTC, junto com e-mails e ligações;
  • o conector do Claude não devolve `hs_all_attendee_emails` (a nekt tem).
Então, depois de 29/09, quem lê é o CONECTOR "HubSpot" do Claude (o app não tem
token do HubSpot desde 08/2026): busca as reuniões do Gong de um dia e depois os
CONTATOS associados a elas. A busca de contatos devolve o conjunto sem dizer qual
contato é de qual reunião — por isso o grão é o DIA, como no WhatsApp; a
inatividade é contada em dias e não perde nada.

Tabelas (dataset `tractian-bi.pepeuhacker`, criadas sob demanda):
  gong_meetings           1 linha por reunião (MERGE por meeting_id)
  gong_meeting_contacts   (meeting_date, contact_id): o contato esteve numa reunião do Gong no dia UTC
  gong_days               dia buscado: n de reuniões e de buscas de contato que ele exige
  gong_day_searches       busca de contato concluída (dia completo = todas as buscas dele)

A view `pepeuhacker.gong_meeting_contacts_live` (sql/) junta isto com a cópia da
nekt (até 29/09, participante pelo e-mail) e é ela que bq._activity_cte lê.

Fluxo por dia D (UTC), o mesmo na carga histórica e na semanal:
  1. plan                                 → dias a buscar
  2. [conector] search MEETING Gong com início no dia D (páginas de 200) → arquivos
  3. meetings D ARQ...                    → gong_meetings + buscas de contato do dia (≤500 reuniões cada)
  4. show D N                             → filterGroups da busca N, pronto pra colar
  5. [conector] search CONTACT com esses filterGroups → arquivos (ou IDs, se inline)
  6. contacts D --search N --expect-total T ARQ...   → gong_meeting_contacts
  7. status                               → dias completos, faltando e contagens
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import warnings
from datetime import date, datetime, timedelta, timezone

warnings.filterwarnings("ignore")

PROJECT = "tractian-bi"
DS = f"{PROJECT}.pepeuhacker"
T_MEET, T_CONT, T_DAYS, T_SRCH = (f"{DS}.gong_meetings", f"{DS}.gong_meeting_contacts",
                                  f"{DS}.gong_days", f"{DS}.gong_day_searches")
# A cópia da nekt cobre até 29/09/2026 17h UTC; o conector assume o dia inteiro.
START = date(2026, 9, 29)
# Toda carga relê os últimos dias já completos: o Gong grava a reunião no
# HubSpot até algumas horas depois, e contato associado depois também entra.
OVERLAP_DAYS = 2
IDS_POR_FILTRO = 100        # limite do associatedWith do conector
FILTROS_POR_BUSCA = 5       # limite de filterGroups por busca
LOTES = os.path.expanduser("~/.pepeuhacker-tools/gong")

_client = None


def bq():
    global _client
    if _client is None:
        from google.cloud import bigquery
        _client = bigquery.Client(project=PROJECT)
    return _client


DDL = [
    f"""CREATE TABLE IF NOT EXISTS `{T_MEET}` (
      meeting_id STRING NOT NULL, meeting_date DATE NOT NULL, start_at TIMESTAMP,
      owner_id STRING, title STRING, loaded_at TIMESTAMP) CLUSTER BY meeting_date""",
    f"""CREATE TABLE IF NOT EXISTS `{T_CONT}` (
      meeting_date DATE NOT NULL, contact_id STRING NOT NULL, loaded_at TIMESTAMP)
    CLUSTER BY contact_id""",
    f"""CREATE TABLE IF NOT EXISTS `{T_DAYS}` (
      meeting_date DATE NOT NULL, n_meetings INT64, n_searches INT64, loaded_at TIMESTAMP)""",
    f"""CREATE TABLE IF NOT EXISTS `{T_SRCH}` (
      meeting_date DATE NOT NULL, search_idx INT64 NOT NULL, n_contacts INT64, loaded_at TIMESTAMP)""",
]


def dml(sql: str, params: list | None = None):
    from google.cloud import bigquery
    return bq().query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params or [])).result()


def ensure_tables():
    for q in DDL:
        dml(q)


def _dia(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _ontem() -> date:
    return datetime.now(timezone.utc).date() - timedelta(days=1)


# ---------------------------------------------------------------- leitura dos arquivos

def ler(path: str) -> tuple[list | None, dict]:
    """Arquivo salvo de um search_crm_objects → (results, {total, offset}).
    Arquivo de texto só com IDs (resultado que veio inline) → (None, {ids})."""
    raw = open(path, encoding="utf-8").read().strip()
    try:
        j = json.loads(raw[raw.find("["):] if raw.lstrip().startswith("[") else raw[raw.find("{"):])
        if isinstance(j, list):                      # [{type:'text', text:'{...}'}]
            j = j[0]
        if isinstance(j, dict) and "text" in j and "results" not in j:
            j = json.loads(j["text"])
        return j.get("results", []), {"total": j.get("total"), "offset": j.get("offset")}
    except (json.JSONDecodeError, KeyError, TypeError, IndexError, AttributeError, ValueError):
        return None, {"ids": re.findall(r"\b\d{5,}\b", raw)}


def _coletar(arquivos):
    """IDs + propriedades de todos os arquivos de UMA busca; total pela paginação."""
    linhas, totais = {}, []
    for f in arquivos:
        res, meta = ler(f)
        if res is None:
            for i in meta["ids"]:
                linhas.setdefault(i, {})
            continue
        if meta.get("total") is not None:
            totais.append(int(meta["total"]))
        for r in res:
            i = str(r.get("id") or (r.get("properties") or {}).get("hs_object_id"))
            linhas[i] = r.get("properties") or {}
    return linhas, totais


def _confere_total(n, totais, esperado, partial_ok):
    if esperado is not None:
        totais = totais + [esperado]
    if totais and max(totais) != n and not partial_ok:
        print(json.dumps({"ok": False, "erro": "contagem não bate com o total da busca "
                          "(faltam páginas ou ID transcrito errado)",
                          "total": max(totais), "lidos": n}))
        sys.exit(2)


# ---------------------------------------------------------------- comandos

def cmd_plan(a):
    """Dias (UTC) a buscar: do último dia completo − OVERLAP até ontem, mais os
    dias incompletos desde START. Hoje fica de fora (dia ainda aberto)."""
    ensure_tables()
    feitos = _dias_completos()
    ate = _dia(a.ate) if a.ate else _ontem()
    if a.desde:
        ini = _dia(a.desde)
    elif feitos:
        ini = max(START, max(feitos) - timedelta(days=OVERLAP_DAYS))
    else:
        ini = START
    dias = []
    d = START
    while d <= ate:
        if d >= ini or d not in feitos:
            dias.append(d.isoformat())
        d += timedelta(days=1)
    print(json.dumps({"dias": dias, "n": len(dias),
                      "ultimo_completo": max(feitos).isoformat() if feitos else None}))


def cmd_meetings(a):
    """Reuniões do Gong de UM dia → gong_meetings e o plano das buscas de contato.
    Zera os contatos do dia: a carga do dia é sempre refeita por inteiro."""
    from google.cloud import bigquery
    ensure_tables()
    d = _dia(a.dia)
    linhas, totais = _coletar(a.files)
    fora = [i for i, p in linhas.items()
            if p.get("hs_meeting_start_time") and p["hs_meeting_start_time"][:10] != a.dia]
    if fora and not a.partial_ok:
        print(json.dumps({"ok": False, "erro": "reunião com início fora do dia (janela errada na busca?)",
                          "ids": fora[:10]}))
        sys.exit(3)
    fontes = {p.get("hs_object_source_detail_1") for p in linhas.values() if p}
    if fontes - {"Gong", None} and not a.partial_ok:
        print(json.dumps({"ok": False, "erro": "reunião que não é do Gong (faltou o filtro de origem?)",
                          "fontes": sorted(str(x) for x in fontes)}))
        sys.exit(3)
    _confere_total(len(linhas), totais, a.expect_total, a.partial_ok)
    ids = sorted(linhas, key=int)
    agora = datetime.now(timezone.utc).isoformat()
    if ids:
        dml(f"""
          MERGE `{T_MEET}` t USING (
            SELECT i AS meeting_id, SAFE_CAST(s AS TIMESTAMP) AS start_at, o AS owner_id, ti AS title
            FROM UNNEST(@ids) i WITH OFFSET k
            JOIN UNNEST(@st) s WITH OFFSET k2 ON k = k2
            JOIN UNNEST(@ow) o WITH OFFSET k3 ON k = k3
            JOIN UNNEST(@ti) ti WITH OFFSET k4 ON k = k4) s
          ON t.meeting_id = s.meeting_id
          WHEN MATCHED THEN UPDATE SET meeting_date = @d, start_at = COALESCE(s.start_at, t.start_at),
            owner_id = COALESCE(NULLIF(s.owner_id, ''), t.owner_id), title = COALESCE(NULLIF(s.title, ''), t.title),
            loaded_at = @agora
          WHEN NOT MATCHED THEN INSERT (meeting_id, meeting_date, start_at, owner_id, title, loaded_at)
            VALUES (s.meeting_id, @d, s.start_at, NULLIF(s.owner_id, ''), NULLIF(s.title, ''), @agora)""",
            [bigquery.ArrayQueryParameter("ids", "STRING", ids),
             bigquery.ArrayQueryParameter("st", "STRING", [linhas[i].get("hs_meeting_start_time") or "" for i in ids]),
             bigquery.ArrayQueryParameter("ow", "STRING", [str(linhas[i].get("hubspot_owner_id") or "") for i in ids]),
             bigquery.ArrayQueryParameter("ti", "STRING", [(linhas[i].get("hs_meeting_title") or "")[:300] for i in ids]),
             bigquery.ScalarQueryParameter("d", "DATE", d),
             bigquery.ScalarQueryParameter("agora", "TIMESTAMP", agora)])
    # Reunião que sumiu do HubSpot (apagada) sai do dia.
    dml(f"DELETE FROM `{T_MEET}` WHERE meeting_date = @d AND meeting_id NOT IN UNNEST(@ids)",
        [bigquery.ScalarQueryParameter("d", "DATE", d), bigquery.ArrayQueryParameter("ids", "STRING", ids)])
    por_busca = IDS_POR_FILTRO * FILTROS_POR_BUSCA
    buscas = []
    for b in range(0, len(ids), por_busca):
        bloco = ids[b:b + por_busca]
        buscas.append([{"associatedWith": [{"objectType": "meetings", "operator": "IN",
                                            "objectIdValues": [int(x) for x in bloco[k:k + IDS_POR_FILTRO]]}]}
                       for k in range(0, len(bloco), IDS_POR_FILTRO)])
    os.makedirs(LOTES, exist_ok=True)
    json.dump({"dia": a.dia, "meetings": ids, "buscas": buscas}, open(f"{LOTES}/{a.dia}.json", "w"))
    for q in (f"DELETE FROM `{T_CONT}` WHERE meeting_date = @d",
              f"DELETE FROM `{T_SRCH}` WHERE meeting_date = @d",
              f"DELETE FROM `{T_DAYS}` WHERE meeting_date = @d"):
        dml(q, [bigquery.ScalarQueryParameter("d", "DATE", d)])
    dml(f"INSERT `{T_DAYS}` (meeting_date, n_meetings, n_searches, loaded_at) VALUES (@d, @n, @b, @agora)",
        [bigquery.ScalarQueryParameter("d", "DATE", d), bigquery.ScalarQueryParameter("n", "INT64", len(ids)),
         bigquery.ScalarQueryParameter("b", "INT64", len(buscas)),
         bigquery.ScalarQueryParameter("agora", "TIMESTAMP", agora)])
    print(json.dumps({"ok": True, "dia": a.dia, "reunioes": len(ids), "buscas_de_contato": len(buscas),
                      "proximo": (f"show {a.dia} 0" if buscas else "dia completo (nenhuma reunião)")}))


def cmd_show(a):
    """filterGroups (JSON compacto) da busca N de contatos do dia."""
    lote = json.load(open(f"{LOTES}/{a.dia}.json"))
    fg = lote["buscas"][a.busca]
    print(json.dumps({"dia": a.dia, "busca": a.busca, "de": len(lote["buscas"]),
                      "reunioes": sum(len(g["associatedWith"][0]["objectIdValues"]) for g in fg),
                      "filterGroups": fg}, separators=(",", ":")))


def cmd_contacts(a):
    """Contatos associados às reuniões da busca N do dia → gong_meeting_contacts.
    Aceita o JSON salvo pelo conector ou um texto só com IDs (resultado inline)."""
    from google.cloud import bigquery
    ensure_tables()
    d = _dia(a.dia)
    lote = json.load(open(f"{LOTES}/{a.dia}.json"))
    if not 0 <= a.busca < len(lote["buscas"]):
        print(json.dumps({"ok": False, "erro": f"o dia tem {len(lote['buscas'])} busca(s)"}))
        sys.exit(2)
    linhas, totais = _coletar(a.files)
    _confere_total(len(linhas), totais, a.expect_total, a.partial_ok)
    # Funcionário da Tractian cadastrado como contato não é retorno de cliente.
    staff = {i for i, p in linhas.items() if (p.get("email") or p.get("hs_full_name_or_email") or "")
             .lower().endswith("@tractian.com")}
    ids = sorted(set(linhas) - staff, key=int)
    # Todo ID tem de existir como contato: pega ID transcrito errado quando o
    # resultado veio inline. A cópia de contatos da nekt atrasa uns dias, então
    # contato recém-criado pode faltar — tolera uma fração pequena.
    conhecidos = {r["c"] for r in bq().query(
        f"""SELECT CAST(CAST(vid AS INT64) AS STRING) c FROM `{PROJECT}.hubspot_nekt.hubspot_contacts_transformed`
            WHERE CAST(CAST(vid AS INT64) AS STRING) IN UNNEST(@ids)""",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ArrayQueryParameter("ids", "STRING", ids)])).result()} if ids else set()
    desconhecidos = [i for i in ids if i not in conhecidos]
    if len(desconhecidos) > max(3, 0.05 * len(ids)) and not a.partial_ok:
        print(json.dumps({"ok": False, "erro": "muitos IDs que não existem como contato (transcrição?)",
                          "n": len(desconhecidos), "ids": desconhecidos[:20]}))
        sys.exit(3)
    agora = datetime.now(timezone.utc).isoformat()
    p = [bigquery.ScalarQueryParameter("d", "DATE", d),
         bigquery.ArrayQueryParameter("ids", "STRING", ids),
         bigquery.ScalarQueryParameter("b", "INT64", a.busca),
         bigquery.ScalarQueryParameter("n", "INT64", len(ids)),
         bigquery.ScalarQueryParameter("agora", "TIMESTAMP", agora)]
    if ids:
        dml(f"""MERGE `{T_CONT}` t USING (SELECT DISTINCT x AS contact_id FROM UNNEST(@ids) x) s
                ON t.meeting_date = @d AND t.contact_id = s.contact_id
                WHEN NOT MATCHED THEN INSERT (meeting_date, contact_id, loaded_at) VALUES (@d, s.contact_id, @agora)""", p)
    dml(f"DELETE FROM `{T_SRCH}` WHERE meeting_date = @d AND search_idx = @b", p)
    dml(f"INSERT `{T_SRCH}` (meeting_date, search_idx, n_contacts, loaded_at) VALUES (@d, @b, @n, @agora)", p)
    feitas = len(list(bq().query(f"SELECT DISTINCT search_idx FROM `{T_SRCH}` WHERE meeting_date = '{d}'").result()))
    completo = feitas >= len(lote["buscas"])
    print(json.dumps({"ok": True, "dia": a.dia, "busca": a.busca, "contatos": len(ids),
                      "staff_fora": len(staff), "desconhecidos": len(desconhecidos),
                      "dia_completo": completo,
                      "proximo": None if completo else f"show {a.dia} {a.busca + 1}"}))


def _dias_completos() -> set:
    q = f"""SELECT d.meeting_date
            FROM `{T_DAYS}` d LEFT JOIN `{T_SRCH}` s USING (meeting_date)
            GROUP BY d.meeting_date, d.n_searches
            HAVING COUNT(DISTINCT s.search_idx) >= d.n_searches"""
    return {r["meeting_date"] for r in bq().query(q).result()}


def cmd_status(a):
    ensure_tables()
    feitos = _dias_completos()
    faltando = []
    d = START
    while d <= _ontem():
        if d not in feitos:
            faltando.append(d.isoformat())
        d += timedelta(days=1)
    r = dict(list(bq().query(f"""
      SELECT
        (SELECT COUNT(*) FROM `{T_MEET}`) reunioes,
        (SELECT COUNT(*) FROM `{T_CONT}`) contato_dias,
        (SELECT COUNT(DISTINCT contact_id) FROM `{T_CONT}`) contatos,
        (SELECT MAX(loaded_at) FROM `{T_DAYS}`) ultima_carga""").result())[0])
    r.update(dias_completos=len(feitos), ultimo_completo=max(feitos).isoformat() if feitos else None,
             faltando=faltando)
    print(json.dumps(r, default=str, ensure_ascii=False, indent=1))


def cmd_total(a):
    """total / offset / n de um arquivo salvo de search_crm_objects."""
    res, meta = ler(a.file)
    print(json.dumps({"n": None if res is None else len(res), **meta} if res is not None
                     else {"n_ids": len(meta["ids"])}))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = ap.add_subparsers(dest="cmd", required=True)
    s = sp.add_parser("plan"); s.add_argument("--desde"); s.add_argument("--ate"); s.set_defaults(f=cmd_plan)
    s = sp.add_parser("meetings"); s.add_argument("dia"); s.add_argument("files", nargs="+")
    s.add_argument("--expect-total", type=int); s.add_argument("--partial-ok", action="store_true")
    s.set_defaults(f=cmd_meetings)
    s = sp.add_parser("show"); s.add_argument("dia"); s.add_argument("busca", type=int); s.set_defaults(f=cmd_show)
    s = sp.add_parser("contacts"); s.add_argument("dia"); s.add_argument("files", nargs="+")
    s.add_argument("--busca", type=int, required=True); s.add_argument("--expect-total", type=int)
    s.add_argument("--partial-ok", action="store_true"); s.set_defaults(f=cmd_contacts)
    s = sp.add_parser("status"); s.set_defaults(f=cmd_status)
    s = sp.add_parser("total"); s.add_argument("file"); s.set_defaults(f=cmd_total)
    a = ap.parse_args()
    a.f(a)


if __name__ == "__main__":
    main()
