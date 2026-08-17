#!/bin/bash
# Publica o pepeuhacker no Cloud Run.
#
# POR QUE ESTE ARQUIVO FOI RECRIADO: o script original estava listado no
# .gcloudignore, então ele nunca subiu junto com o fonte e não veio no pacote
# recuperado do Cloud Run — perdeu-se com o disco do laptop. As flags abaixo
# foram extraídas do serviço que está NO AR (revisão pepeuhacker-00003-q2t),
# então um deploy com este script reproduz a configuração atual em vez de
# sobrescrevê-la com os padrões do gcloud.
set -euo pipefail

GCLOUD="$HOME/google-cloud-sdk/bin/gcloud"
# O gcloud exige Python 3.10+; o do sistema é 3.9.6. Este 3.12 é só pra ele.
export CLOUDSDK_PYTHON="$HOME/.local/python312/bin/python3"

cd "$(dirname "$0")"

echo "Publicando pepeuhacker · projeto tractian-bi · us-central1"
echo "ATENÇÃO: este serviço é público (allUsers). Confira o que está no código antes."
read -r -p "Continuar? [s/N] " ok
[ "$ok" = "s" ] || { echo "Cancelado."; exit 1; }

"$GCLOUD" run deploy pepeuhacker \
  --source . \
  --project tractian-bi \
  --region us-central1 \
  --service-account senior-leadership@tractian-bi.iam.gserviceaccount.com \
  --allow-unauthenticated \
  --min-instances 1 \
  --max-instances 3 \
  --cpu 1 \
  --memory 2Gi \
  --timeout 300 \
  --cpu-boost \
  --set-env-vars PEPEU_CACHE_DIR=/var/cache/pepeuhacker,USD_BRL_RATE=5.05

# --min-instances 1: o cache são 125 parquets em tmpfs (RAM). Se a instância
# reciclar, tudo é reconstruído do BigQuery no próximo acesso — manter uma
# instância viva evita pagar essa reconstrução no cold start.
