#!/bin/bash
# Monta o zip que se manda pra quem quer o conector: a ponte (tools/mcp_local.py),
# o instalar.sh e o LEIA-ME.md. Sai em dist/pepeuhacker-conector.zip.
# Rodar de novo depois de mexer na ponte, senão o kit vai com a versão velha.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

KIT="$TMP/pepeuhacker-conector"
mkdir -p "$KIT" "$REPO/dist"
cp "$REPO/tools/mcp_local.py" "$REPO/tools/kit_conector/instalar.sh" \
   "$REPO/tools/kit_conector/LEIA-ME.md" "$KIT/"
chmod +x "$KIT/instalar.sh"

rm -f "$REPO/dist/pepeuhacker-conector.zip"
(cd "$TMP" && zip -qrX "$REPO/dist/pepeuhacker-conector.zip" pepeuhacker-conector)
echo "$REPO/dist/pepeuhacker-conector.zip"
