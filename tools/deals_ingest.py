"""Carga do ESTADO ATUAL dos deals do HubSpot (via conector do Claude) no BigQuery.

Existe por causa de 13/10/2026: as duas fontes de deals do HubSpot no BigQuery
pararam em 11/10 às 20h01 BRT — o conector da nekt seguia rodando (extração às
14h22 UTC) mas não trazia nenhuma alteração posterior, e a legada já estava
parada desde 08/10. Close date, estágio e forecast category mudados depois
disso não chegavam ao dashboard (ex.: Usina Estiva - Expansão Smart, com close
date 31/12/2026 no HubSpot e vazio no BigQuery).

O app não tem token do HubSpot; quem lê é o conector "HubSpot" do Claude, que
devolve páginas de 200 deals salvas em arquivo (JSON com `results`). Este script
lê esses arquivos e faz MERGE em `tractian-bi.pepeuhacker.hubspot_deals_mcp`,
uma linha por deal, ficando com a versão de `hs_lastmodifieddate` mais nova.
A view `hubspot_deals_live` mescla essa tabela com as outras duas e fica com a
linha mais recente de cada deal — quando a nekt voltar, ela ganha sozinha.

Uso:
    python tools/deals_ingest.py load <arquivo.json> [<arquivo.json> ...]
    python tools/deals_ingest.py status
"""
import json
import sys
from datetime import datetime, timezone

from google.cloud import bigquery

TABELA = "tractian-bi.pepeuhacker.hubspot_deals_mcp"
SCHEMA = [
    bigquery.SchemaField("dealId", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("dealname", "STRING"),
    bigquery.SchemaField("pipeline", "STRING"),
    bigquery.SchemaField("dealstage", "STRING"),
    bigquery.SchemaField("closedate", "TIMESTAMP"),
    bigquery.SchemaField("hs_manual_forecast_category", "STRING"),
    bigquery.SchemaField("amount", "FLOAT"),
    bigquery.SchemaField("hubspot_owner_id", "STRING"),
    bigquery.SchemaField("hs_lastmodifieddate", "TIMESTAMP"),
    bigquery.SchemaField("carregado_em", "TIMESTAMP"),
]


def _ler(caminho):
    raw = open(caminho, encoding="utf-8").read()
    i = raw.find("{")
    d = json.loads(raw[i:])
    return d.get("results", []), d.get("total")


def _linha(r, agora):
    p = r.get("properties") or {}
    amt = p.get("amount")
    try:
        amt = float(amt) if amt not in (None, "") else None
    except ValueError:
        amt = None
    return {
        "dealId": str(r.get("id") or p.get("hs_object_id")),
        "dealname": p.get("dealname"),
        "pipeline": p.get("pipeline"),
        "dealstage": p.get("dealstage"),
        "closedate": p.get("closedate") or None,
        "hs_manual_forecast_category": p.get("hs_manual_forecast_category") or None,
        "amount": amt,
        "hubspot_owner_id": p.get("hubspot_owner_id"),
        "hs_lastmodifieddate": p.get("hs_lastmodifieddate"),
        "carregado_em": agora,
    }


def load(arquivos):
    agora = datetime.now(timezone.utc).isoformat()
    por_deal = {}
    for a in arquivos:
        res, total = _ler(a)
        print(f"  {a.split('/')[-1]}: {len(res)} deals (total da busca {total})")
        for r in res:
            ln = _linha(r, agora)
            ant = por_deal.get(ln["dealId"])
            if ant is None or (ln["hs_lastmodifieddate"] or "") >= (ant["hs_lastmodifieddate"] or ""):
                por_deal[ln["dealId"]] = ln
    linhas = list(por_deal.values())
    c = bigquery.Client(project="tractian-bi")
    c.create_table(bigquery.Table(TABELA, schema=SCHEMA), exists_ok=True)
    stage = TABELA + "_stage"
    c.load_table_from_json(
        linhas, stage,
        job_config=bigquery.LoadJobConfig(schema=SCHEMA, write_disposition="WRITE_TRUNCATE"),
    ).result()
    # MERGE: só substitui quando a versão nova é tão ou mais recente.
    q = c.query(f"""
    MERGE `{TABELA}` t
    USING `{stage}` s ON t.dealId = s.dealId
    WHEN MATCHED AND (t.hs_lastmodifieddate IS NULL OR s.hs_lastmodifieddate >= t.hs_lastmodifieddate) THEN
      UPDATE SET dealname = s.dealname, pipeline = s.pipeline, dealstage = s.dealstage,
                 closedate = s.closedate, hs_manual_forecast_category = s.hs_manual_forecast_category,
                 amount = s.amount, hubspot_owner_id = s.hubspot_owner_id,
                 hs_lastmodifieddate = s.hs_lastmodifieddate, carregado_em = s.carregado_em
    WHEN NOT MATCHED THEN INSERT ROW
    """)
    q.result()
    c.delete_table(stage, not_found_ok=True)
    print(json.dumps({"deals_lidos": len(linhas), "linhas_afetadas": q.num_dml_affected_rows}))


def status():
    c = bigquery.Client(project="tractian-bi")
    r = list(c.query(f"""
      SELECT COUNT(*) n, MAX(hs_lastmodifieddate) ultimo_mod, MAX(carregado_em) ultima_carga,
             COUNTIF(pipeline = 'default') br, COUNTIF(pipeline = '2395281') mx
      FROM `{TABELA}`""").result())[0]
    print(json.dumps({k: str(v) for k, v in dict(r).items()}))


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "load":
        load(sys.argv[2:])
    elif len(sys.argv) == 2 and sys.argv[1] == "status":
        status()
    else:
        print(__doc__)
        sys.exit(1)
