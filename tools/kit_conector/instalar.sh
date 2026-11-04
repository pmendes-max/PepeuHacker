#!/bin/bash
# Instala o conector do pepeuhacker (dashboard de vendas da Tractian) no Claude
# Desktop e, se existir, no Claude Code. Roda no Mac de quem recebeu o kit:
#
#   bash ~/Downloads/pepeuhacker-conector/instalar.sh
#
# O que faz: cria um Python isolado em ~/.pepeuhacker-conector, testa o acesso
# com o login Google de quem roda (precisa ser @tractian.com) e registra o
# servidor MCP "pepeuhacker". Pode rodar de novo quantas vezes quiser.
set -euo pipefail

AQUI="$(cd "$(dirname "$0")" && pwd)"
DESTINO="$HOME/.pepeuhacker-conector"
PY="$DESTINO/venv/bin/python"
PONTE="$DESTINO/mcp_local.py"
ADC="$HOME/.config/gcloud/application_default_credentials.json"
DESKTOP_CFG="$HOME/Library/Application Support/Claude/claude_desktop_config.json"

passo() { printf '\n\033[1m%s\033[0m\n' "$1"; }
falha() { printf '\n\033[31m%s\033[0m\n' "$1" >&2; exit 1; }

# ── 1. Python ────────────────────────────────────────────────────────────────
passo "1/5 · Python"
PY_SISTEMA="$(command -v python3 || true)"
[ -n "$PY_SISTEMA" ] || falha "Não achei python3. Instale com: xcode-select --install"
"$PY_SISTEMA" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)' \
  || falha "Preciso de Python 3.9 ou mais novo ($("$PY_SISTEMA" --version 2>&1))."
echo "usando $("$PY_SISTEMA" --version 2>&1) em $PY_SISTEMA"

mkdir -p "$DESTINO"
[ -x "$PY" ] || "$PY_SISTEMA" -m venv "$DESTINO/venv"
"$PY" -m pip install --quiet --disable-pip-version-check --upgrade google-auth requests
cp "$AQUI/mcp_local.py" "$PONTE"
echo "ponte em $PONTE"

# ── 2. Login Google ──────────────────────────────────────────────────────────
passo "2/5 · Login Google (@tractian.com)"
GCLOUD="$(command -v gcloud || true)"
for c in /opt/homebrew/bin/gcloud /usr/local/bin/gcloud \
         /opt/homebrew/share/google-cloud-sdk/bin/gcloud "$HOME/google-cloud-sdk/bin/gcloud"; do
  [ -n "$GCLOUD" ] && break
  [ -x "$c" ] && GCLOUD="$c"
done

login() {
  [ -n "$GCLOUD" ] || falha "Falta o gcloud. Instale com: brew install --cask google-cloud-sdk
(sem Homebrew: https://cloud.google.com/sdk/docs/install) e rode este script de novo."
  echo "Vai abrir o navegador: entre com a conta @tractian.com e aceite."
  "$GCLOUD" auth application-default login
}

[ -f "$ADC" ] && echo "login já existe em $ADC" || login

# ── 3. Teste ─────────────────────────────────────────────────────────────────
passo "3/5 · Testando o acesso ao dashboard"
testar() {
  local saida
  saida="$(printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' \
    | "$PY" "$PONTE" --modo remoto 2>"$DESTINO/ultimo_teste.log" || true)"
  [[ "$saida" == *'"listar_telas"'* ]]
}
if ! testar; then
  echo "Não passou ($(tail -1 "$DESTINO/ultimo_teste.log")). Refazendo o login…"
  login
  testar || falha "Acesso recusado. Confira se entrou com a conta @tractian.com.
Detalhe em $DESTINO/ultimo_teste.log — mande pro Pedro Mendes."
fi
echo "ok: o dashboard respondeu"

# ── 4. Claude Desktop ────────────────────────────────────────────────────────
passo "4/5 · Claude Desktop"
mkdir -p "$(dirname "$DESKTOP_CFG")"
[ -f "$DESKTOP_CFG" ] && cp "$DESKTOP_CFG" "$DESKTOP_CFG.antes-pepeuhacker"
"$PY" - "$DESKTOP_CFG" "$PY" "$PONTE" <<'EOF'
import json, os, sys
caminho, py, ponte = sys.argv[1:]
cfg = {}
if os.path.exists(caminho) and os.path.getsize(caminho):
    with open(caminho) as f:
        cfg = json.load(f)
cfg.setdefault("mcpServers", {})["pepeuhacker"] = {"command": py, "args": [ponte, "--modo", "remoto"]}
with open(caminho, "w") as f:
    json.dump(cfg, f, indent=2, ensure_ascii=False)
EOF
echo "registrado em $DESKTOP_CFG"

# ── 5. Claude Code (opcional) ────────────────────────────────────────────────
passo "5/5 · Claude Code"
if command -v claude >/dev/null; then
  claude mcp remove pepeuhacker --scope user >/dev/null 2>&1 || true
  claude mcp add pepeuhacker --scope user -- "$PY" "$PONTE" --modo remoto
else
  echo "Claude Code não instalado; pulei."
fi

passo "Pronto. Feche o Claude Desktop (Cmd+Q) e abra de novo."
echo "Depois, numa conversa: \"no pepeuhacker, qual o forecast do trimestre?\""
