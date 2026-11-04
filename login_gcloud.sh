#!/bin/bash
# Faz o login do Pedro na conta Google (passo que só ele pode dar).
#
# POR QUE ESTE SCRIPT EXISTE: mandar "rode gcloud auth login" falha se o Terminal
# já estava ABERTO antes de o ~/.zshrc ser editado — a sessão antiga não conhece o
# comando e devolve `command not found: gcloud`. Aqui os caminhos vão explícitos,
# então funciona em qualquer terminal, novo ou velho, sem depender de config.
set -u

GCLOUD="$HOME/google-cloud-sdk/bin/gcloud"

if [ ! -x "$GCLOUD" ]; then
  echo "ERRO: gcloud não encontrado em $GCLOUD" >&2
  exit 1
fi

# O gcloud exige Python 3.10+; o do sistema é 3.9.6.
#
# CORRIGIDO EM 18/08/2026: este script apontava para
# ~/.pepeuhacker-tools/python/bin/python3, que NÃO EXISTE (o 3.12 standalone
# mora em ~/.local/python312; o ~/.pepeuhacker-tools só tem o venv do app).
# Como o `[ -x ]` dava falso, CLOUDSDK_PYTHON ficava vazio, o gcloud caía no
# 3.9 do sistema e morria com "gcloud failed to load" ANTES de qualquer login.
# O script então falhava por inteiro — rodá-lo não mudava nada.
# Agora falha ALTO se não achar um Python compatível, em vez de seguir quieto.
for c in "$HOME/.local/python312/bin/python3" "$HOME/.pepeuhacker-tools/venv/bin/python3"; do
  [ -x "$c" ] && { export CLOUDSDK_PYTHON="$c"; break; }
done
if [ -z "${CLOUDSDK_PYTHON:-}" ]; then
  echo "ERRO: nenhum Python 3.10+ encontrado para o gcloud." >&2
  echo "Procurei em ~/.local/python312 e ~/.pepeuhacker-tools/venv." >&2
  exit 1
fi

# Checa se o login está MESMO válido pedindo um token de verdade.
#
# CORRIGIDO EM 18/08/2026: antes isto usava `auth list`, que continua listando a
# conta como ACTIVE mesmo depois de a Tractian forçar reautenticação — o token
# está morto, mas a conta aparece. O script dizia "Você JÁ está logado", saía com
# exit 0 e nunca chamava o `auth login`. Pedir o token é o único teste honesto:
# ele falha exatamente quando o reauth está pendente.
if "$GCLOUD" auth print-access-token >/dev/null 2>&1; then
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
if "$GCLOUD" auth print-access-token >/dev/null 2>&1; then
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
