#!/bin/bash
# AMBIENTE ONLINE — fecha o site atrás de login Google restrito à Tractian.
#
# SITUAÇÃO HOJE: a política IAM do serviço concede roles/run.invoker a
# `allUsers`. Qualquer pessoa com a URL vê meta, faturamento e pipeline, sem
# login. Como o link já circulou em DMs do Slack, ele está efetivamente público.
#
# POR QUE IAP E NÃO SÓ IAM: trocar `allUsers` por `domain:tractian.com` no
# run.invoker NÃO resolve — o Cloud Run passaria a exigir um header
# `Authorization: Bearer ...` que o navegador não manda, e todo mundo veria 403.
# O Identity-Aware Proxy fica NA FRENTE do serviço: quem abre a URL recebe a
# tela de login do Google e, se o e-mail for @tractian.com, entra normalmente.
#
# Uso:
#   ./setup_sso.sh            aplica
#   ./setup_sso.sh --rollback volta a ser público (reverte tudo)
set -euo pipefail

GCLOUD="$HOME/google-cloud-sdk/bin/gcloud"
export CLOUDSDK_PYTHON="$HOME/.local/python312/bin/python3"
PROJETO="tractian-bi"
NUMERO="199700483619"
SERVICO="pepeuhacker"
REGIAO="us-central1"
URL="https://pepeuhacker-vn4vurgqda-uc.a.run.app"

if [ "${1:-}" = "--rollback" ]; then
  echo "REVERTENDO: o site volta a ficar PÚBLICO para qualquer pessoa da internet."
  read -r -p "Confirma? [s/N] " ok
  [ "$ok" = "s" ] || { echo "Cancelado."; exit 1; }
  "$GCLOUD" run services update "$SERVICO" --no-iap --region "$REGIAO" --project "$PROJETO"
  "$GCLOUD" run services add-iam-policy-binding "$SERVICO" --region "$REGIAO" \
    --project "$PROJETO" --member=allUsers --role=roles/run.invoker
  echo "Revertido — site público de novo."
  exit 0
fi

cat <<EOF
========================================================================
 FECHAR O SITE COM SSO DA TRACTIAN
========================================================================
 Depois disto:
   - quem abrir $URL
     cai na tela de login do Google;
   - entra quem tiver e-mail @tractian.com;
   - qualquer outra pessoa recebe "acesso negado".

 Durante a troca há alguns segundos em que ninguém acessa. Se algo der
 errado, rode:  ./setup_sso.sh --rollback
========================================================================
EOF
read -r -p "Aplicar? [s/N] " ok
[ "$ok" = "s" ] || { echo "Cancelado."; exit 1; }

echo
echo "[1/4] Habilitando o IAP no serviço..."
"$GCLOUD" run services update "$SERVICO" --iap --region "$REGIAO" --project "$PROJETO"

echo
echo "[2/4] Garantindo que o IAP possa invocar o serviço..."
# O IAP chama o Cloud Run como este service agent. Sem run.invoker aqui, o login
# funciona e ainda assim todo mundo toma 403 na porta seguinte.
"$GCLOUD" run services add-iam-policy-binding "$SERVICO" --region "$REGIAO" \
  --project "$PROJETO" \
  --member="serviceAccount:service-${NUMERO}@gcp-sa-iap.iam.gserviceaccount.com" \
  --role=roles/run.invoker >/dev/null
echo "     ok"

echo
echo "[3/4] Liberando o domínio tractian.com..."
"$GCLOUD" iap web add-iam-policy-binding \
  --resource-type=cloud-run --service="$SERVICO" --region="$REGIAO" \
  --project="$PROJETO" --member="domain:tractian.com" \
  --role=roles/iap.httpsResourceAccessor >/dev/null
echo "     ok — qualquer e-mail @tractian.com entra"

echo
echo "[4/4] Removendo o acesso público (allUsers)..."
"$GCLOUD" run services remove-iam-policy-binding "$SERVICO" --region "$REGIAO" \
  --project "$PROJETO" --member=allUsers --role=roles/run.invoker >/dev/null
echo "     ok"

echo
echo "=== VERIFICAÇÃO ==="
# Sem login, o esperado agora é 302 para accounts.google.com — não 200 (aberto)
# nem 403 (quebrado para todo mundo, inclusive quem tem acesso).
CODE=$(curl -sS -o /dev/null -w '%{http_code}' "$URL/leaders/overview")
DEST=$(curl -sS -o /dev/null -w '%{redirect_url}' "$URL/leaders/overview")
echo "sem login: HTTP $CODE"
[ -n "$DEST" ] && echo "redireciona para: ${DEST:0:60}..."
case "$CODE" in
  302|303) echo "OK — o site está pedindo login do Google." ;;
  200)     echo "ATENÇÃO: ainda respondeu 200. O IAP pode levar 1-2 min para propagar; teste de novo." ;;
  403)     echo "ATENÇÃO: 403. Verifique o passo [3/4] ou rode ./setup_sso.sh --rollback." ;;
  *)       echo "Código inesperado. Se o time estiver sem acesso, rode ./setup_sso.sh --rollback." ;;
esac

echo
echo "Abra $URL numa aba anônima para confirmar o login."
echo "Para reverter a qualquer momento: ./setup_sso.sh --rollback"
