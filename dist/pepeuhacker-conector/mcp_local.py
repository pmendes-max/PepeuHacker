#!/usr/bin/env python3
"""Ponte MCP (stdio) do pepeuhacker — o "conector" que Claude Desktop, Claude Code
e outros clientes MCP chamam pra ler o dashboard sem clicar em nada.

Dois modos, a mesma API (as ferramentas de agent_api.py):

  local            Sobe o app DENTRO deste processo e responde pelo /mcp dele.
                   O BigQuery é lido com as credenciais ADC de quem roda
                   (`gcloud auth application-default login`), como no run_dev.sh.
                   Não passa pelo Cloud Run nem pelo IAP. Os números são os
                   mesmos da tela; a idade do dado pode diferir alguns minutos
                   do site, porque os caches são outros (o selo `freshness` diz).

  remoto           Repassa cada mensagem pro /mcp do gêmeo `pepeuhacker-api`
                   (mesmo app, sem IAP, protegido pelo IAM do Cloud Run) com um
                   token Google de quem roda. Não precisa do repositório na
                   máquina, só deste arquivo e de google-auth + requests.
                   O principal (com IAP) recusa token de linha de comando; por
                   isso o gêmeo existe — ver AGENTES.md.

Sem --modo: local quando o arquivo está dentro do repositório (tem app.py ao
lado), remoto quando foi mandado sozinho no kit (tools/kit_conector).

POR QUE stdout É SAGRADO: no transporte stdio, stdout carrega SÓ mensagens do
protocolo. O app e o bq.py imprimem log em stdout ("bq · refresh bg ..."), e uma
linha dessas no meio quebra o cliente. Por isso o descritor 1 é duplicado pro
protocolo e o resto do processo passa a escrever em stderr.

Uso (Claude Desktop / Claude Code):
  command: ~/.pepeuhacker-tools/venv/bin/python
  args:    [~/Documents/pepeuhacker/tools/mcp_local.py]          # local
           [..., "--modo", "remoto"]                             # remoto
"""
import argparse
import base64
import concurrent.futures
import json
import os
import sys
import threading
import time
import warnings

# O google-auth avisa a cada chamada que ADC de usuário não tem quota project;
# pra leitura do dashboard isso não importa, e o aviso só polui o log do cliente.
warnings.filterwarnings("ignore", message="Your application has authenticated using end user credentials")
# No Python 3.9 do macOS (o do kit), google-auth avisa fim de vida e o urllib3
# reclama do LibreSSL; nada disso impede a ponte, e o instalador mostra a
# última linha do log quando o teste falha: ela tem que ser o erro de verdade.
warnings.filterwarnings("ignore", category=FutureWarning, module="google")
warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL")

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
URL_PADRAO = "https://pepeuhacker-api-199700483619.us-central1.run.app"   # o gêmeo, não o principal

# stdout do protocolo, separado ANTES de qualquer import que possa imprimir.
_PROTO = os.fdopen(os.dup(1), "w", encoding="utf-8", buffering=1)
os.dup2(2, 1)
sys.stdout = sys.stderr
_LOCK = threading.Lock()


def log(msg):
    print(f"[pepeuhacker-mcp] {msg}", file=sys.stderr, flush=True)


def responder(obj):
    linha = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    with _LOCK:
        _PROTO.write(linha + "\n")
        _PROTO.flush()


def erro_rpc(id_, codigo, msg):
    return {"jsonrpc": "2.0", "id": id_, "error": {"code": codigo, "message": msg}}


# ── Modo local ────────────────────────────────────────────────────────────────

class Local:
    def __init__(self):
        sys.path.insert(0, REPO)
        os.chdir(REPO)
        t0 = time.time()
        from app import app          # noqa: E402 — import pesado de propósito aqui
        self.client = app.test_client()
        log(f"app carregado em {time.time() - t0:.1f}s (modo local, BigQuery via ADC)")

    def enviar(self, msg):
        r = self.client.post("/mcp", json=msg)
        if r.status_code == 202:
            return None
        return r.get_json()


# ── Modo remoto ───────────────────────────────────────────────────────────────

class Remoto:
    """Token Google de quem roda: usuário (ADC) → id_token do próprio login;
    service account → id_token com audience = URL (ou PEPEU_MCP_AUDIENCE)."""

    def __init__(self, url):
        import google.auth
        import google.auth.transport.requests
        import requests
        self.url = url.rstrip("/") + "/mcp"
        self.audience = os.environ.get("PEPEU_MCP_AUDIENCE") or url.rstrip("/")
        self.http = requests.Session()
        self.req = google.auth.transport.requests.Request()
        self.creds, _ = google.auth.default(scopes=[
            "openid", "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/cloud-platform"])
        self.token, self.exp = None, 0
        self.tlock = threading.Lock()

    def _token(self):
        with self.tlock:
            if self.token and self.exp - time.time() > 300:
                return self.token
            if hasattr(self.creds, "service_account_email"):
                from google.oauth2 import id_token
                tok = id_token.fetch_id_token(self.req, self.audience)
            else:
                self.creds.refresh(self.req)
                tok = getattr(self.creds, "id_token", None)
                if not tok:
                    raise RuntimeError("as credenciais ADC não trouxeram id_token; rode "
                                       "`gcloud auth application-default login`")
            corpo = tok.split(".")[1]
            self.exp = json.loads(base64.urlsafe_b64decode(corpo + "=" * (-len(corpo) % 4)))["exp"]
            self.token = tok
            return tok

    def enviar(self, msg):
        r = self.http.post(self.url, json=msg, timeout=300, allow_redirects=False, headers={
            "Authorization": "Bearer " + self._token(),
            "Accept": "application/json, text/event-stream"})
        if r.status_code == 202:
            return None
        if r.status_code in (301, 302, 401, 403):
            raise RuntimeError(f"acesso recusado ({r.status_code}). Confira se a conta tem run.invoker "
                               "no pepeuhacker-api e se o login ADC está válido "
                               "(`gcloud auth application-default login`). Ver AGENTES.md.")
        r.raise_for_status()
        return r.json()


# ── Laço stdio ────────────────────────────────────────────────────────────────

def tratar(ponte, linha):
    try:
        msg = json.loads(linha)
    except json.JSONDecodeError:
        responder(erro_rpc(None, -32700, "JSON inválido"))
        return
    id_ = msg.get("id") if isinstance(msg, dict) else None
    try:
        resp = ponte.enviar(msg)
    except Exception as e:
        log(f"falha: {e}")
        if id_ is not None:
            responder(erro_rpc(id_, -32603, str(e)))
        return
    if resp is None:
        return
    for r in (resp if isinstance(resp, list) else [resp]):
        responder(r)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    # Sem o repositório ao lado (o arquivo foi mandado sozinho, no kit), só o remoto funciona.
    padrao = "local" if os.path.exists(os.path.join(REPO, "app.py")) else "remoto"
    ap.add_argument("--modo", choices=["local", "remoto"], default=os.environ.get("PEPEU_MCP_MODO", padrao))
    ap.add_argument("--url", default=os.environ.get("PEPEU_MCP_URL", URL_PADRAO))
    a = ap.parse_args()
    ponte = Local() if a.modo == "local" else Remoto(a.url)
    # Chamadas em paralelo: uma tela fria pode levar dezenas de segundos no
    # BigQuery, e um ping do cliente não pode ficar na fila atrás dela.
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        for linha in sys.stdin:
            if linha.strip():
                pool.submit(tratar, ponte, linha)


if __name__ == "__main__":
    main()
