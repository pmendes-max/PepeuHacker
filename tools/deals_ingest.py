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

Desde 11/2026 o app faz esta carga sozinho (`hs_sync.py`, com o HUBSPOT_TOKEN).
Este script fica como plano B manual, pelo conector do Claude.

Uso:
    python tools/deals_ingest.py load <arquivo.json> [<arquivo.json> ...]
    python tools/deals_ingest.py status
"""
import json
import sys
from pathlib import Path
from datetime import datetime, timezone

from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import hs_sync  # noqa: E402 — mesmas colunas e o mesmo MERGE do sincronizador automático

TABELA = hs_sync.TABELA


def _ler(caminho):
    raw = open(caminho, encoding="utf-8").read()
    i = raw.find("{")
    d = json.loads(raw[i:])
    return d.get("results", []), d.get("total")


def load(arquivos):
    agora = datetime.now(timezone.utc).isoformat()
    linhas = []
    for a in arquivos:
        res, total = _ler(a)
        print(f"  {a.split('/')[-1]}: {len(res)} deals (total da busca {total})")
        linhas += [hs_sync.linha(r, agora) for r in res]
    n = hs_sync.merge(linhas)
    print(json.dumps({"deals_lidos": len(linhas), "linhas_afetadas": n}))


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
