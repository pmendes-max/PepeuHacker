#!/bin/bash
# AMBIENTE DE DESENVOLVIMENTO — só nesta máquina, só para você.
#
# Sobe na porta 8081 (e não na 8080) de propósito: assim você pode manter a
# versão "de produção local" rodando na 8080 e comparar as duas lado a lado sem
# uma derrubar a outra com "Address already in use".
#
# Diferenças em relação ao app.py rodado direto:
#   - recarrega sozinho a cada save (--debug), sem precisar reiniciar na mão;
#   - erro vira traceback na tela do navegador em vez de página 500 muda;
#   - PULA o _prewarm(): o boot é instantâneo e as páginas saem do cache em
#     disco. Se quiser dado fresco do BigQuery, use o botão "↻ Refresh BQ".
#
# Escuta em 127.0.0.1: o app NÃO tem autenticação nenhuma, então ele não pode
# ficar exposto na rede local. Quem precisa acessar de fora usa o site online,
# que tem SSO (ver setup_sso.sh).
set -euo pipefail

cd "$(dirname "$0")"
PY="$HOME/.pepeuhacker-tools/venv/bin/python"

# Token do HubSpot, se existir. Sem ele o app roda igual — os dados de HubSpot
# dos dashboards vêm do BigQuery; só o recurso de consulta à API ao vivo fica
# inativo, e o próprio código trata isso com is_configured().
[ -f "$HOME/.config/hubspot/env" ] && source "$HOME/.config/hubspot/env"

echo "dev · http://127.0.0.1:8081  (Ctrl+C para parar, recarrega a cada save)"
exec "$PY" -m flask --app app run --port 8081 --debug
