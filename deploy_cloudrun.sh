#!/bin/bash
# Publica o pepeuhacker no Cloud Run — MESMO serviço, MESMO link.
#
# Copia a CONFIGURAÇÃO da revisão que está servindo o tráfego agora (service
# account, variáveis de ambiente, secrets, CPU/memória, timeout, concorrência,
# min/max instâncias, cpu-boost, VPC) e publica o código DESTA pasta com ela.
# Depois move 100% do tráfego para a revisão nova e imprime o comando de volta.
#
# POR QUE ASSIM (04/11/2026): a versão anterior deste script tinha flags fixas
# copiadas da revisão 00003 (ago/2026) e `--set-env-vars`, que APAGA as demais
# variáveis. Publicou a 00076 por cima da 00075 com config e código antigos.
# E um `gcloud run deploy` sem flags também não serve: ele herda o TEMPLATE do
# serviço (= a última revisão publicada, a 00076), não a que está no ar.
#
# ANTES DE PUBLICAR: esta pasta tem de estar com o código certo
# (`git checkout main && git pull`).
set -euo pipefail

SERVICE=pepeuhacker
PROJECT=tractian-bi
REGION=us-central1
GCLOUD="$HOME/google-cloud-sdk/bin/gcloud"
export CLOUDSDK_PYTHON="${CLOUDSDK_PYTHON:-$HOME/.local/python312/bin/python3}"
PY="$CLOUDSDK_PYTHON"

cd "$(dirname "$0")"
G=("$GCLOUD" --project "$PROJECT")

# 1) Revisão que está servindo 100% do tráfego agora (a referência de config).
SVC_JSON="$("${G[@]}" run services describe "$SERVICE" --region "$REGION" --format=json)"
REF_REV="${REF_REV:-$(printf '%s' "$SVC_JSON" | "$PY" -c '
import json,sys
t=json.load(sys.stdin).get("status",{}).get("traffic",[])
r=[x.get("revisionName") for x in t if x.get("percent")==100 and x.get("revisionName")]
print(r[0] if r else "")')}"
[ -n "$REF_REV" ] || { echo "ERRO: não achei a revisão com 100% do tráfego. Rode com REF_REV=<revisão> $0"; exit 1; }

# 2) Config dessa revisão → flags do deploy (env vars vão num arquivo temporário).
TMPD="$(mktemp -d)"; trap 'rm -rf "$TMPD"' EXIT
"${G[@]}" run revisions describe "$REF_REV" --region "$REGION" --format=json > "$TMPD/rev.json"
"$PY" - "$TMPD" <<'PYEOF'
import json, sys, os
d = sys.argv[1]
rev = json.load(open(os.path.join(d, "rev.json")))
spec, meta = rev.get("spec", {}), rev.get("metadata", {})
ann = meta.get("annotations", {}) or {}
c = (spec.get("containers") or [{}])[0]
flags, env, secrets = [], {}, []
for e in c.get("env", []) or []:
    if "value" in e:
        env[e["name"]] = e["value"]
    elif "valueFrom" in e and "secretKeyRef" in e["valueFrom"]:
        k = e["valueFrom"]["secretKeyRef"]
        secrets.append(f'{e["name"]}={k["name"]}:{k.get("key","latest")}')
if spec.get("volumes"):
    print("ABORT: a revisão de referência tem volumes montados; este script não os copia.")
    sys.exit(3)
with open(os.path.join(d, "env.yaml"), "w") as f:
    for k, v in env.items():
        f.write(f"{k}: {json.dumps(str(v))}\n")
if env:
    flags.append(f"--env-vars-file={os.path.join(d, 'env.yaml')}")
else:
    flags.append("--clear-env-vars")
if secrets:
    flags.append("--set-secrets=" + ",".join(secrets))
if spec.get("serviceAccountName"):
    flags.append(f"--service-account={spec['serviceAccountName']}")
lim = (c.get("resources") or {}).get("limits") or {}
if lim.get("cpu"):    flags.append(f"--cpu={lim['cpu']}")
if lim.get("memory"): flags.append(f"--memory={lim['memory']}")
if spec.get("timeoutSeconds"):      flags.append(f"--timeout={spec['timeoutSeconds']}")
if spec.get("containerConcurrency"): flags.append(f"--concurrency={spec['containerConcurrency']}")
if "autoscaling.knative.dev/minScale" in ann: flags.append(f"--min-instances={ann['autoscaling.knative.dev/minScale']}")
if "autoscaling.knative.dev/maxScale" in ann: flags.append(f"--max-instances={ann['autoscaling.knative.dev/maxScale']}")
if ann.get("run.googleapis.com/startup-cpu-boost") == "true": flags.append("--cpu-boost")
else: flags.append("--no-cpu-boost")
if ann.get("run.googleapis.com/vpc-access-connector"):
    flags.append(f"--vpc-connector={ann['run.googleapis.com/vpc-access-connector']}")
    if ann.get("run.googleapis.com/vpc-access-egress"):
        flags.append(f"--vpc-egress={ann['run.googleapis.com/vpc-access-egress']}")
open(os.path.join(d, "flags"), "w").write("\n".join(flags))
print("Variáveis de ambiente copiadas:", ", ".join(sorted(env)) or "(nenhuma)")
print("Secrets copiados:", ", ".join(s.split("=")[0] for s in secrets) or "(nenhum)")
print("Demais:", " ".join(x for x in flags if not x.startswith(("--env-vars-file", "--set-secrets"))))
PYEOF
mapfile -t FLAGS < "$TMPD/flags" 2>/dev/null || IFS=$'\n' read -r -d '' -a FLAGS < "$TMPD/flags" || true

echo
echo "Publicando $SERVICE · $PROJECT · $REGION"
echo "Config copiada da revisão no ar: $REF_REV"
echo "Código desta pasta: $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo '?') · $(git log -1 --format='%h %s' 2>/dev/null || echo '?')"
read -r -p "Continuar? [s/N] " ok
[ "$ok" = "s" ] || { echo "Cancelado."; exit 1; }

# 3) Publica sem tráfego, depois move 100% para a revisão nova.
"${G[@]}" run deploy "$SERVICE" --source . --region "$REGION" --no-traffic "${FLAGS[@]}"
"${G[@]}" run services update-traffic "$SERVICE" --region "$REGION" --to-latest

echo
echo "Publicado. Se algo der errado, volte para a revisão anterior (o link não muda):"
echo "  $GCLOUD run services update-traffic $SERVICE --to-revisions=$REF_REV=100 --region $REGION --project $PROJECT"
