#!/bin/bash
# Publica o gêmeo `pepeuhacker-api`: o mesmo app, pra agentes de IA fora do Mac.
#
# POR QUE UM SEGUNDO SERVIÇO: o principal fica atrás do IAP, que só aceita login
# de navegador. Token Google de linha de comando volta "Invalid JWT audience", e
# a lista de clientes programáticos do IAP só vale no nível do PROJETO — e o
# tractian-bi é compartilhado (o fse-frontend, de outro time, também está atrás
# do IAP e herdaria a mudança). O gêmeo não tem IAP: quem protege é o IAM do
# Cloud Run (`run.invoker`), que aceita token de usuário e de service account.
# Detalhes em AGENTES.md.
#
# MESMA IMAGEM DO PRINCIPAL: não há build aqui. O gêmeo sobe com a imagem da
# revisão que está no ar no `pepeuhacker`, então os dois servem o mesmo código.
# Rode depois de cada ./deploy_cloudrun.sh.
#
# QUEM ENTRA: `domain:tractian.com`, o mesmo público do IAP do principal. Agente
# em nuvem entra com uma service account própria:
#   gcloud run services add-iam-policy-binding pepeuhacker-api --region us-central1 \
#     --project tractian-bi --member=serviceAccount:<sa> --role=roles/run.invoker
set -euo pipefail

GCLOUD="$HOME/google-cloud-sdk/bin/gcloud"
export CLOUDSDK_PYTHON="$HOME/.local/python312/bin/python3"
PROJETO="tractian-bi"
REGIAO="us-central1"
SERVICO="pepeuhacker-api"
# Cliente OAuth do ADC do gcloud (`gcloud auth application-default login`). O
# id_token do ADC sai com ESTE audience, e não com a URL do serviço; sem ele na
# lista de audiences aceitos, a ponte local de cada pessoa tomaria 401.
AUD_ADC="764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur.apps.googleusercontent.com"

cd "$(dirname "$0")"

IMG=$("$GCLOUD" run services describe pepeuhacker --region "$REGIAO" --project "$PROJETO" \
        --format='value(spec.template.spec.containers[0].image)')
[ -n "$IMG" ] || { echo "Não achei a imagem do pepeuhacker."; exit 1; }
echo "Publicando $SERVICO com a imagem do principal:"
echo "  $IMG"
read -r -p "Continuar? [s/N] " ok
[ "$ok" = "s" ] || { echo "Cancelado."; exit 1; }

# --min-instances 0: agente chama em rajada; ocioso, não custa nada. O preço é o
#   boot frio (o cache nasce vazio e a 1ª tela de cada tipo vai ao BigQuery).
# PEPEU_PAPEL=api: sem _prewarm e sem os laços que gravam as fotos do forecast
#   (ver o __main__ do app.py) — quem grava é o principal.
# --no-cpu-throttling: o cache vencido é renovado numa thread em segundo plano
#   (stale-while-revalidate do bq.query); sem CPU fora do request ela não anda.
"$GCLOUD" run deploy "$SERVICO" \
  --image "$IMG" \
  --project "$PROJETO" \
  --region "$REGIAO" \
  --service-account senior-leadership@tractian-bi.iam.gserviceaccount.com \
  --no-allow-unauthenticated \
  --no-iap \
  --min-instances 0 \
  --max-instances 2 \
  --cpu 1 \
  --memory 2Gi \
  --timeout 300 \
  --cpu-boost \
  --no-cpu-throttling \
  --add-custom-audiences "$AUD_ADC" \
  --set-env-vars PEPEU_PAPEL=api,PEPEU_CACHE_DIR=/var/cache/pepeuhacker,USD_BRL_RATE=5.05

"$GCLOUD" run services add-iam-policy-binding "$SERVICO" --region "$REGIAO" \
  --project "$PROJETO" --member="domain:tractian.com" --role=roles/run.invoker >/dev/null

# Confere que o gêmeo NÃO está aberto: sem token o esperado é 401/403. 200 aqui
# quer dizer que alguém concedeu run.invoker a allUsers.
echo
echo "=== VERIFICAÇÃO DE ACESSO ==="
URL=$("$GCLOUD" run services describe "$SERVICO" --region "$REGIAO" --project "$PROJETO" --format='value(status.url)')
CODE=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$URL/mcp" || echo "erro")
case "$CODE" in
  401|403) echo "OK — HTTP $CODE sem token: só entra quem tem run.invoker." ;;
  *)       echo "!!! HTTP $CODE SEM TOKEN — confira o IAM do $SERVICO AGORA."
           echo "!!! Esperado 401/403. Remova allUsers/allAuthenticatedUsers do run.invoker." ;;
esac
echo "URL: $URL"
