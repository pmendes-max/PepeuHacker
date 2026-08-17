#!/bin/bash
# Reconfigura o HUBSPOT_TOKEN — local e em produção.
#
# POR QUE ESTE ARQUIVO EXISTE: o token ficava em ~/.config/hubspot/env, FORA do
# repositório, e se perdeu junto com o disco do laptop. O Secret Manager do
# tractian-bi só tem o HAPIKEY de 2022 (autenticação legada que o HubSpot
# desativou), então não há nada a recuperar: é preciso gerar um Private App novo.
#
# O token é digitado por VOCÊ, direto aqui, e não passa por chat nem por log:
# `read -s` não ecoa na tela e o valor vai direto pro Secret Manager e pro
# arquivo local com permissão 600.
#
# ANTES DE RODAR, crie o Private App em:
#   HubSpot → Settings → Integrations → Private Apps → Create private app
# com estes escopos (exigidos pelo hubspot.py):
#   crm.objects.calls.read
#   crm.objects.emails.read
#   crm.objects.meetings.read
#   crm.objects.contacts.read
#   crm.objects.owners.read
set -euo pipefail

GCLOUD="$HOME/google-cloud-sdk/bin/gcloud"
export CLOUDSDK_PYTHON="$HOME/.local/python312/bin/python3"
PROJETO="tractian-bi"
SA="senior-leadership@tractian-bi.iam.gserviceaccount.com"

echo "Cole o token do Private App (formato pat-na2-...). Nada será exibido na tela."
read -r -s -p "HUBSPOT_TOKEN: " TOKEN
echo

case "$TOKEN" in
  pat-*) : ;;
  "")   echo "ERRO: vazio. Abortado."; exit 1 ;;
  *)    echo "AVISO: não começa com 'pat-'. O hubspot.py espera um Private App token."
        read -r -p "Continuar assim mesmo? [s/N] " ok
        [ "$ok" = "s" ] || { echo "Abortado."; exit 1; } ;;
esac

# ---- 1. Local: o caminho que o hubspot.py procura -------------------------
mkdir -p "$HOME/.config/hubspot"
umask 177   # cria o arquivo já como 600; sem isto ele nasce legível por outros
printf "export HUBSPOT_TOKEN='%s'\n" "$TOKEN" > "$HOME/.config/hubspot/env"
echo "OK  local: ~/.config/hubspot/env (permissão 600)"

# ---- 2. Secret Manager: para produção ler sem embutir o token na imagem ----
if "$GCLOUD" secrets describe HUBSPOT_TOKEN --project "$PROJETO" >/dev/null 2>&1; then
  printf '%s' "$TOKEN" | "$GCLOUD" secrets versions add HUBSPOT_TOKEN \
      --project "$PROJETO" --data-file=- >/dev/null
  echo "OK  Secret Manager: nova versão de HUBSPOT_TOKEN"
else
  printf '%s' "$TOKEN" | "$GCLOUD" secrets create HUBSPOT_TOKEN \
      --project "$PROJETO" --replication-policy=automatic --data-file=- >/dev/null
  echo "OK  Secret Manager: secret HUBSPOT_TOKEN criado"
fi

unset TOKEN   # tira da memória do shell assim que deixa de ser necessário

# O serviço roda como esta identidade; sem este papel ele não lê o secret.
"$GCLOUD" secrets add-iam-policy-binding HUBSPOT_TOKEN \
  --project "$PROJETO" --member="serviceAccount:$SA" \
  --role=roles/secretmanager.secretAccessor >/dev/null 2>&1 || true
echo "OK  acesso concedido a $SA"

# ---- 3. Cloud Run: injeta por REFERÊNCIA, não por valor -------------------
echo
echo "Falta ligar o serviço ao secret. Isso cria uma revisão nova em produção:"
echo
echo "  $GCLOUD run services update pepeuhacker \\"
echo "    --region us-central1 --project $PROJETO \\"
echo "    --set-secrets HUBSPOT_TOKEN=HUBSPOT_TOKEN:latest"
echo
read -r -p "Rodar agora? [s/N] " ok
if [ "$ok" = "s" ]; then
  "$GCLOUD" run services update pepeuhacker \
    --region us-central1 --project "$PROJETO" \
    --set-secrets HUBSPOT_TOKEN=HUBSPOT_TOKEN:latest
  echo "OK  produção lendo o token do Secret Manager."
else
  echo "Pulado. Rode o comando acima quando quiser."
fi

echo
echo "Para usar localmente, carregue no shell antes de subir o app:"
echo "  source ~/.config/hubspot/env"
