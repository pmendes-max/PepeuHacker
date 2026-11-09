"""Ponte direta HubSpot → BigQuery para os deals (sincronizador automático).

POR QUE EXISTE (11/2026): desde 11/10/2026 nenhuma sync automática do HubSpot
chega ao BigQuery (legada e nekt pararam). A única fonte viva da view
`pepeuhacker.hubspot_deals_live` virou `pepeuhacker.hubspot_deals_mcp`, que só
era carregada À MÃO (conector do Claude + `tools/deals_ingest.py`) — e quando
ninguém rodava, o selo "HubSpot" envelhecia dias (04/11 → 09/11).

Este módulo faz a mesma carga sozinho, pela API do HubSpot com o HUBSPOT_TOKEN
do Private App, de 10 em 10 min, dentro do próprio app (`_hs_sync_loop`):

  1. marca d'água = MAX(hs_lastmodifieddate) já gravado − 15 min de folga;
  2. busca no HubSpot os deals dos funis Sales BR e Sales MX alterados desde ali
     (Search API, 200 por página, em ordem crescente de alteração);
  3. MERGE na tabela, ficando com a versão mais nova de cada deal.

Só o que mudou trafega: numa volta normal são dezenas de deals, segundos de
API. A tabela segue no mesmo desempate por `hs_lastmodifieddate` da view, então
nada no resto do app precisa mudar para o selo e o funil ficarem em dia.

Também grava valor, dono, data de criação, data do PO e tipo do deal — o que
hoje vem da base do BI (`int_adding_names_to_deals`). Ficam aqui prontos para a
fase seguinte: tirar o app da dependência dessa base.

Uso manual (sem o app):
    HUBSPOT_TOKEN=... python hs_sync.py           # uma volta incremental
    HUBSPOT_TOKEN=... python hs_sync.py --desde 2026-10-11
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta, timezone

import requests

API = "https://api.hubapi.com"
TABELA = "tractian-bi.pepeuhacker.hubspot_deals_mcp"
PIPELINES = ("default", "2395281")          # Sales BR, Sales MX
# Sem nada gravado ainda: começa no dia em que as syncs automáticas pararam.
INICIO_PADRAO = datetime(2026, 10, 11, tzinfo=timezone.utc)
FOLGA = timedelta(minutes=15)               # relógio do HubSpot x indexação da busca
LIMITE_BUSCA = 10_000                       # teto da Search API por consulta

# propriedade do HubSpot -> (coluna, tipo BigQuery)
CAMPOS = {
    "dealname":                    ("dealname", "STRING"),
    "pipeline":                    ("pipeline", "STRING"),
    "dealstage":                   ("dealstage", "STRING"),
    "closedate":                   ("closedate", "TIMESTAMP"),
    "hs_manual_forecast_category": ("hs_manual_forecast_category", "STRING"),
    "amount":                      ("amount", "FLOAT"),
    "hubspot_owner_id":            ("hubspot_owner_id", "STRING"),
    "hs_lastmodifieddate":         ("hs_lastmodifieddate", "TIMESTAMP"),
    # Novos (11/2026) — base para sair da dependência da curated do BI.
    "createdate":                  ("createdate", "TIMESTAMP"),
    "data_de_emissao_po":          ("po_emission_date", "DATE"),
    "deal_type":                   ("deal_type", "STRING"),
    "amount_mrr":                  ("amount_mrr", "FLOAT"),
    "amount_in_home_currency":     ("amount_in_home_currency", "FLOAT"),
    "deal_currency_code":          ("deal_currency_code", "STRING"),
    "customer_owner":              ("customer_owner_id", "STRING"),
    "sdr":                         ("sdr_owner_id", "STRING"),
}


class HubSpotError(Exception):
    pass


def _token() -> str:
    import os
    t = os.environ.get("HUBSPOT_TOKEN")
    if not t:
        raise HubSpotError("HUBSPOT_TOKEN não configurado (ver setup_hubspot.sh)")
    return t


def _schema():
    from google.cloud import bigquery
    cols = [bigquery.SchemaField("dealId", "STRING", mode="REQUIRED")]
    cols += [bigquery.SchemaField(c, t) for c, t in CAMPOS.values()]
    cols.append(bigquery.SchemaField("carregado_em", "TIMESTAMP"))
    return cols


def _client():
    from google.cloud import bigquery
    return bigquery.Client(project="tractian-bi")


_TABELA_OK = False


def garantir_tabela(c=None) -> None:
    """Cria a tabela, ou acrescenta as colunas novas a uma já existente. Uma vez
    por processo: o laço roda de 10 em 10 min e o DDL não precisa ir junto."""
    global _TABELA_OK
    if _TABELA_OK:
        return
    from google.cloud import bigquery
    c = c or _client()
    c.create_table(bigquery.Table(TABELA, schema=_schema()), exists_ok=True)
    adds = ", ".join(f"ADD COLUMN IF NOT EXISTS {col} {tipo}"
                     for col, tipo in list(CAMPOS.values()) + [("carregado_em", "TIMESTAMP")])
    c.query(f"ALTER TABLE `{TABELA}` {adds}").result()
    _TABELA_OK = True


def marca_dagua(c=None) -> datetime:
    c = c or _client()
    rows = list(c.query(f"SELECT MAX(hs_lastmodifieddate) FROM `{TABELA}`").result())
    ultimo = rows[0][0] if rows else None
    return (ultimo - FOLGA) if ultimo else INICIO_PADRAO


def _num(v):
    try:
        return float(v) if v not in (None, "") else None
    except ValueError:
        return None


def linha(r: dict, agora: str) -> dict:
    """Resultado da API do HubSpot -> linha da tabela. Também serve aos arquivos
    salvos pelo conector do Claude (`tools/deals_ingest.py`), que têm o mesmo shape."""
    p = r.get("properties") or {}
    out = {"dealId": str(r.get("id") or p.get("hs_object_id")), "carregado_em": agora}
    for prop, (col, tipo) in CAMPOS.items():
        v = p.get(prop)
        if v in ("", None):
            v = None
        elif tipo == "FLOAT":
            v = _num(v)
        elif tipo == "DATE":
            v = str(v)[:10]
        out[col] = v
    return out


def merge(linhas: list[dict], c=None) -> int:
    """MERGE por dealId: só substitui quando a versão nova é tão ou mais recente."""
    from google.cloud import bigquery
    if not linhas:
        return 0
    c = c or _client()
    garantir_tabela(c)
    por_deal: dict[str, dict] = {}
    for ln in linhas:
        ant = por_deal.get(ln["dealId"])
        if ant is None or (ln["hs_lastmodifieddate"] or "") >= (ant["hs_lastmodifieddate"] or ""):
            por_deal[ln["dealId"]] = ln
    stage = TABELA + "_stage"
    c.load_table_from_json(
        list(por_deal.values()), stage,
        job_config=bigquery.LoadJobConfig(schema=_schema(), write_disposition="WRITE_TRUNCATE"),
    ).result()
    cols = [col for col, _ in CAMPOS.values()] + ["carregado_em"]
    sets = ", ".join(f"{col} = s.{col}" for col in cols)
    q = c.query(f"""
    MERGE `{TABELA}` t
    USING `{stage}` s ON t.dealId = s.dealId
    WHEN MATCHED AND (t.hs_lastmodifieddate IS NULL OR s.hs_lastmodifieddate >= t.hs_lastmodifieddate) THEN
      UPDATE SET {sets}
    WHEN NOT MATCHED THEN
      INSERT (dealId, {", ".join(cols)}) VALUES (s.dealId, {", ".join("s." + x for x in cols)})
    """)
    q.result()
    c.delete_table(stage, not_found_ok=True)
    return int(q.num_dml_affected_rows or 0)


def _post(path: str, body: dict, tentativa: int = 0) -> dict:
    r = requests.post(f"{API}{path}", json=body, timeout=30,
                      headers={"Authorization": f"Bearer {_token()}"})
    if r.status_code == 429 or (r.status_code >= 500 and tentativa < 3):
        if tentativa >= 5:
            raise HubSpotError(f"HubSpot {r.status_code} em {path} após {tentativa} tentativas")
        time.sleep(int(r.headers.get("Retry-After", 0) or 2 ** tentativa))
        return _post(path, body, tentativa + 1)
    if r.status_code >= 400:
        raise HubSpotError(f"HubSpot {r.status_code} em {path}: {r.text[:300]}")
    return r.json()


def buscar_alterados(desde: datetime) -> list[dict]:
    """Todos os deals BR/MX com hs_lastmodifieddate >= `desde`.

    A Search API devolve no máximo 10 mil resultados por consulta. Como a ordem é
    crescente de alteração, ao bater o teto a busca recomeça do último instante
    visto — assim a 1ª carga (dezenas de milhares de deals) também cabe."""
    out: list[dict] = []
    cursor = desde
    while True:
        body = {
            "filterGroups": [{"filters": [
                {"propertyName": "pipeline", "operator": "IN", "values": list(PIPELINES)},
                {"propertyName": "hs_lastmodifieddate", "operator": "GTE",
                 "value": str(int(cursor.timestamp() * 1000))},
            ]}],
            "properties": list(CAMPOS),
            "sorts": [{"propertyName": "hs_lastmodifieddate", "direction": "ASCENDING"}],
            "limit": 200,
        }
        vistos, after, ultimo = 0, None, None
        while True:
            if after:
                body["after"] = after
            resp = _post("/crm/v3/objects/deals/search", body)
            res = resp.get("results", [])
            out.extend(res)
            vistos += len(res)
            if res:
                ultimo = (res[-1].get("properties") or {}).get("hs_lastmodifieddate")
            after = (resp.get("paging") or {}).get("next", {}).get("after")
            time.sleep(0.25)                 # Search API: ~5 req/s por app
            if not after or vistos + 200 > LIMITE_BUSCA:
                break
        if not after or not ultimo:
            return out
        novo = datetime.fromisoformat(ultimo.replace("Z", "+00:00"))
        if novo <= cursor:                   # >10 mil deals no mesmo milissegundo: não avança
            return out
        cursor = novo


def sincronizar(desde: datetime | None = None) -> dict:
    """Uma volta completa: marca d'água → API → MERGE. Devolve um resumo."""
    t0 = time.time()
    c = _client()
    garantir_tabela(c)
    desde = desde or marca_dagua(c)
    deals = buscar_alterados(desde)
    agora = datetime.now(timezone.utc).isoformat()
    n = merge([linha(r, agora) for r in deals], c)
    return {"desde": desde.isoformat(), "deals_lidos": len(deals),
            "linhas_afetadas": n, "segundos": round(time.time() - t0, 1)}


if __name__ == "__main__":
    desde = None
    if len(sys.argv) == 3 and sys.argv[1] == "--desde":
        desde = datetime.fromisoformat(sys.argv[2]).replace(tzinfo=timezone.utc)
    elif len(sys.argv) > 1:
        print(__doc__)
        sys.exit(1)
    print(sincronizar(desde))
