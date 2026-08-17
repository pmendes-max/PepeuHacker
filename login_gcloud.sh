#!/bin/bash
# Faz o login do Pedro na conta Google (passo que só ele pode dar).
#
# POR QUE ESTE SCRIPT EXISTE: mandar "rode gcloud auth login" falha se o Terminal
# já estava ABERTO antes de o ~/.zshrc ser editado — a sessão antiga não conhece o
# comando e devolve `command not found: gcloud`. Aqui os caminhos vão explícitos,
# então funciona em qualquer terminal, novo ou velho, sem depender de config.
set -u

GCLOUD="$HOME/google-cloud-sdk/bin/gcloud"
PY="$HOME/.pepeuhacker-tools/python/bin/python3"

if [ ! -x "$GCLOUD" ]; then
  echo "ERRO: gcloud não encontrado em $GCLOUD" >&2
  exit 1
fi
# O gcloud exige Python 3.10+; o do sistema é 3.9.6. Este 3.12 é só pra ele.
[ -x "$PY" ] && export CLOUDSDK_PYTHON="$PY"

if "$GCLOUD" auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null | grep -q .; then
  echo "Você JÁ está logado como: $("$GCLOUD" auth list --filter=status:ACTIVE --format='value(account)')"
  echo "Nada a fazer. Pode seguir para o deploy."
  exit 0
fi

cat <<'EOF'
========================================================================
 LOGIN NA SUA CONTA GOOGLE
========================================================================
 Vai abrir o navegador. Faça assim:

   1. Escolha a conta  pmendes@tractian.com
   2. Clique em "Permitir" / "Allow"
   3. Volte para esta janela

 Sua senha NÃO passa por mim — o login é direto com a Google.
========================================================================

EOF

"$GCLOUD" auth login

echo
if "$GCLOUD" auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null | grep -q .; then
  CONTA="$("$GCLOUD" auth list --filter=status:ACTIVE --format='value(account)')"
  echo "OK — logado como: $CONTA"
  "$GCLOUD" config set project tractian-bi 2>/dev/null
  echo "Projeto definido: tractian-bi"
  echo
  echo "PRONTO. Agora me avise no chat que eu publico o site."
else
  echo "O login não foi concluído. Rode este script de novo, ou me diga o que apareceu na tela."
  exit 1
fi
