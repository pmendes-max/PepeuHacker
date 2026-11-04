# SQL de infraestrutura (fora do app)

Objetos do BigQuery que o dashboard depende mas que NÃO nascem do código Python.
Ficam aqui versionados porque já se perderam uma vez junto com o disco do laptop
(17/08/2026) e porque, ao contrário do app, eles não têm tarball de deploy
guardado em lugar nenhum.

Para aplicar qualquer arquivo deste diretório:

    export PATH="$HOME/google-cloud-sdk/bin:$PATH"
    export CLOUDSDK_PYTHON="$HOME/.local/python312/bin/python3"
    export CLOUDSDK_AUTH_ACCESS_TOKEN=$(gcloud auth application-default print-access-token)
    bq query --use_legacy_sql=false --project_id=tractian-bi < sql/<arquivo>.sql

| arquivo | o que é |
|---|---|
| `hubspot_deals_live.sql` | a view de ESTADO dos deals (estágio, close date, forecast category) que todas as telas leem. Mescla as duas fontes vivas do HubSpot e fica com a linha mais recente de cada deal. Trocar de fonte é editar e reaplicar este arquivo — o app não precisa de deploy. |
| `hubspot_deals_history_live.sql` | histórico versionado de estágio e amount por deal, no shape da `hubspot.hubspot_deals_history` (que parou em 01/10/2026). Mescla a nekt (`analytics_canonical_hubspot_deals_history`, formato longo) com a legada. Base do PoP Early/Late, Histórico PoP, Funil, Funil WoW, Stale e Legal. |
| `hubspot_deals_stage_live.sql` | estágio ATUAL + data de entrada em cada estágio (hs_v2_date_entered_*), BR e MX, mesclando as duas fontes vivas. Base da Inatividade e do Stale desde 08/10/2026, quando `hubspot.hubspot_deals_history` parou de atualizar. |
| `gong_meeting_contacts_live.sql` | contato × reunião do Gong — canal 'gong' da Inatividade desde 19/10/2026. Junta a cópia da nekt de `hubspot_meetings` (até 29/09, participante pelo e-mail) com `pepeuhacker.gong_meeting_contacts`, que `tools/gong_ingest.py` grava pelo conector na rotina de domingo (grão de dia UTC). |
| `forecast_snapshots.sql` | tabela da foto SEMANAL do forecast por deal, gravada pelo refresher do app (best-effort). |
| `forecast_snapshots_daily.sql` | tabela da foto do forecast à MEIA-NOITE BRT de cada dia — as pontas do DoD do Forecast (Leaders). Gravada pelo app (`_midnight_loop`) logo depois da virada; a service account precisa de `dataEditor` no dataset `pepeuhacker`, que já tem. |
