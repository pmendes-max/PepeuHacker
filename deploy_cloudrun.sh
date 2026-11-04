#!/bin/bash
# Publica o pepeuhacker no Cloud Run — MESMO serviço, MESMO link.
#
# Só atualiza o CÓDIGO. Toda configuração do serviço (service account, variáveis
# de ambiente, memória, instâncias, acesso/IAP) é herdada da revisão que está no
# ar: `gcloud run deploy` mantém o que não for passado como flag.
#
# POR QUE ASSIM: a versão anterior deste script passava --set-env-vars,
# --service-account etc. com valores copiados da revisão 00003 (ago/2026). Em
# 04/11/2026 isso publicou a 00076 por cima da 00075 com configuração e código
# antigos; foi preciso voltar o tráfego pra 00075. Não reintroduzir flags de
# configuração aqui — mudança de config se faz de propósito, à parte.
#
# ANTES DE PUBLICAR: este diretório tem de estar com o código de PRODUÇÃO
# (`git checkout main && git pull`). Publicar de uma pasta velha sobrescreve o
# site com código velho.
set -euo pipefail

GCLOUD="$HOME/google-cloud-sdk/bin/gcloud"
# O gcloud exige Python 3.10+; o do sistema é 3.9.
export CLOUDSDK_PYTHON="${CLOUDSDK_PYTHON:-$HOME/.local/python312/bin/python3}"

cd "$(dirname "$0")"

echo "Publicando pepeuhacker · projeto tractian-bi · us-central1"
echo "Branch: $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo '?')  ·  commit: $(git log -1 --format='%h %s' 2>/dev/null || echo '?')"
read -r -p "Continuar? [s/N] " ok
[ "$ok" = "s" ] || { echo "Cancelado."; exit 1; }

"$GCLOUD" run deploy pepeuhacker \
  --source . \
  --project tractian-bi \
  --region us-central1

echo
echo "Se algo der errado, volte pra revisão anterior (o link não muda):"
echo "  $GCLOUD run revisions list --service pepeuhacker --region us-central1 --project tractian-bi --limit 5"
echo "  $GCLOUD run services update-traffic pepeuhacker --to-revisions=<REVISAO_ANTERIOR>=100 --region us-central1 --project tractian-bi"
