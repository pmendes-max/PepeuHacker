# pepeuhacker no Cloud Run — o dashboard deixa de depender do laptop.
#
# POR QUE ISSO EXISTE: rodando no MacBook o site fica fora do ar sempre que a
# máquina dorme (medido: 144 min de sono seguidos) ou é desligada. Nenhum túnel,
# vigia ou URL fixa serve tráfego de um computador desligado. Aqui o app roda
# num serviço gerenciado, com URL HTTPS permanente e sem sono.
FROM python:3.11-slim

# Python 3.11 (o laptop tem 3.9): bq.py já tem `from __future__ import
# annotations`, e app.py evita `X | None` em runtime por causa do 3.9 — nada
# disso quebra numa versão mais nova, então subir é seguro e tira o app de uma
# versão que já está em fim de vida.
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8080 \
    PEPEU_CACHE_DIR=/var/cache/pepeuhacker

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py bq.py hubspot.py wa_notes.py ./
COPY templates/ ./templates/
COPY static/ ./static/

# Cache de parquet em disco GRAVÁVEL. No Cloud Run é tmpfs (RAM) e some quando a
# instância recicla — por isso o deploy usa --min-instances=1, pra não pagar
# reconstrução de 125 parquets a cada cold start. Pra cache realmente persistente,
# montar um bucket GCS neste caminho (ver comentário no fim de deploy_cloudrun.sh).
RUN mkdir -p /var/cache/pepeuhacker

# NÃO copiamos a chave do service account pra dentro da imagem de propósito:
# chave em imagem vaza junto com a imagem. No Cloud Run a identidade vem do
# service account do serviço, e o google-cloud-bigquery a pega por ADC sozinho
# (bq.py só define GOOGLE_APPLICATION_CREDENTIALS se o arquivo local existir).

EXPOSE 8080
CMD ["python", "-W", "ignore", "app.py"]
