#!/bin/bash
# Restringe o pepeuhacker às pessoas listadas em acesso.txt.
#
# Antes: qualquer @tractian.com entrava (IAP com `domain:tractian.com`).
# Depois: só quem está em acesso.txt — no site (IAP) E no gêmeo dos agentes
# (`pepeuhacker-api`, run.invoker). Os dois precisam andar juntos: fechar só o
# site deixaria os mesmos números abertos pela porta dos agentes.
#
# É uma SINCRONIZAÇÃO: o arquivo é a fonte da verdade. Quem sai do arquivo perde
# o acesso na próxima rodada. Service accounts (agentes em nuvem) no gêmeo NÃO
# são mexidas — continuam como estão.
#
# Ordem segura: primeiro concede os novos, depois remove os antigos — ninguém da
# lista fica sem acesso no meio da troca.
#
# Uso:
#   ./setup_acesso.sh             mostra o que vai mudar e aplica
#   ./setup_acesso.sh --dominio   volta a liberar qualquer @tractian.com
set -euo pipefail

GCLOUD="$HOME/google-cloud-sdk/bin/gcloud"
export CLOUDSDK_PYTHON="$HOME/.local/python312/bin/python3"
PROJETO="tractian-bi"
REGIAO="us-central1"
SITE="pepeuhacker"
API="pepeuhacker-api"
PAPEL_IAP="roles/iap.httpsResourceAccessor"
PAPEL_RUN="roles/run.invoker"

cd "$(dirname "$0")"

iap() { "$GCLOUD" iap web "$@" --resource-type=cloud-run --service="$SITE" \
          --region="$REGIAO" --project="$PROJETO"; }
run() { "$GCLOUD" run services "$@" "$API" --region "$REGIAO" --project "$PROJETO"; }

# Membros atuais de um papel (só pessoas/grupos/domínio — nunca service account).
atuais_iap() { iap get-iam-policy --flatten='bindings[].members' \
  --filter="bindings.role=$PAPEL_IAP" --format='value(bindings.members)' \
  | grep -E '^(user|group|domain):' || true; }
atuais_api() { run get-iam-policy --flatten='bindings[].members' \
  --filter="bindings.role=$PAPEL_RUN" --format='value(bindings.members)' \
  | grep -E '^(user|group|domain):' || true; }

if [ "${1:-}" = "--dominio" ]; then
  DESEJADOS="domain:tractian.com"
else
  [ -f acesso.txt ] || { echo "Falta acesso.txt."; exit 1; }
  DESEJADOS=$(sed -e 's/#.*//' -e 's/[[:space:]]//g' acesso.txt | grep -v '^$' \
    | tr 'A-Z' 'a-z' \
    | sed -E '/^(user|group|domain):/!s/^/user:/' | sort -u)
  [ -n "$DESEJADOS" ] || { echo "acesso.txt vazio — isso trancaria todo mundo. Abortado."; exit 1; }
  # Trava contra se trancar pra fora.
  EU=$("$GCLOUD" config get-value account 2>/dev/null | tr 'A-Z' 'a-z')
  if ! grep -qx "user:$EU" <<<"$DESEJADOS"; then
    echo "ATENÇÃO: você ($EU) não está em acesso.txt e vai perder o acesso."
    read -r -p "Continuar mesmo assim? [s/N] " ok
    [ "$ok" = "s" ] || { echo "Cancelado."; exit 1; }
  fi
fi

sincroniza() {   # $1 = nome, $2 = atuais, $3 = função add, $4 = função remove
  local nome="$1" atuais="$2" add="$3" rem="$4"
  local novos sair
  novos=$(comm -13 <(sort -u <<<"$atuais") <(sort -u <<<"$DESEJADOS") | grep -v '^$' || true)
  sair=$(comm -23 <(sort -u <<<"$atuais") <(sort -u <<<"$DESEJADOS") | grep -v '^$' || true)
  echo "── $nome"
  [ -z "$novos$sair" ] && { echo "   nada a mudar"; return; }
  for m in $novos; do echo "   + $m"; done
  for m in $sair;  do echo "   - $m"; done
  if [ "${APLICAR:-0}" = 1 ]; then
    for m in $novos; do "$add" "$m"; done
    for m in $sair;  do "$rem" "$m"; done
  fi
}
add_iap() { iap add-iam-policy-binding    --member="$1" --role="$PAPEL_IAP" >/dev/null; }
rem_iap() { iap remove-iam-policy-binding --member="$1" --role="$PAPEL_IAP" >/dev/null; }
add_api() { run add-iam-policy-binding    --member="$1" --role="$PAPEL_RUN" >/dev/null; }
rem_api() { run remove-iam-policy-binding --member="$1" --role="$PAPEL_RUN" >/dev/null; }

A_IAP=$(atuais_iap); A_API=$(atuais_api)
echo "=== O QUE VAI MUDAR ==="
sincroniza "site (IAP)"          "$A_IAP" add_iap rem_iap
sincroniza "agentes ($API)"      "$A_API" add_api rem_api
echo
read -r -p "Aplicar? [s/N] " ok
[ "$ok" = "s" ] || { echo "Cancelado."; exit 1; }

APLICAR=1
sincroniza "site (IAP)"          "$A_IAP" add_iap rem_iap
sincroniza "agentes ($API)"      "$A_API" add_api rem_api

echo
echo "Feito. Pode levar 1-2 min pra propagar."
echo "Quem foi removido e estava com a aba aberta perde o acesso quando a sessão"
echo "do IAP renovar (até ~1h). Pra testar: aba anônima com um e-mail fora da lista."
