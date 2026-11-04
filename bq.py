"""BigQuery client with parquet-based cache."""
from __future__ import annotations
import os, re, time, hashlib, threading, warnings
warnings.filterwarnings("ignore")
# Chave local do service account. SÓ aponta a variável se o arquivo EXISTIR:
# fora do laptop (Cloud Run/VM) a identidade vem do próprio ambiente via ADC, e
# apontar GOOGLE_APPLICATION_CREDENTIALS pra um caminho inexistente faz o ADC
# falhar em vez de cair no fallback — o app subiria e morreria na 1ª query.
_LOCAL_SA = os.path.expanduser("~/.config/gcp/tractian-bi.json")
if os.path.exists(_LOCAL_SA):
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", _LOCAL_SA)

from pathlib import Path
import re
import pandas as pd
import db_dtypes  # noqa: F401 — registra dtypes dbdate/dbtime p/ ler parquet do cache
from google.cloud import bigquery

PROJECT = "tractian-bi"
# Diretório do cache configurável: em container a pasta do código é read-only, e
# o cache precisa de disco gravável (ou um volume/GCS montado).
CACHE_DIR = Path(os.environ.get("PEPEU_CACHE_DIR") or (Path(__file__).parent / "cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_TTL = 600  # 10min — refresh automático (stale-while-revalidate no acesso);
                 # botão "↻ Refresh BQ" força atualizar na hora quando precisa

# Circuit breaker: depois de uma falha de BQ, para de re-tentar por BQ_COOLDOWN
# segundos e serve cache na hora (sem pagar o round-trip que vai falhar de novo).
# Auto-recupera: passado o cooldown, a próxima query tenta o BQ de verdade.
BQ_COOLDOWN = 120


def _write_parquet_atomic(df: pd.DataFrame, cache_file: Path) -> None:
    """Grava o parquet em um temporário no MESMO diretório e renomeia por cima.
    POR QUE: `to_parquet` direto no arquivo final o TRUNCA e reescreve no lugar.
    Quem lê durante a escrita pega um parquet pela metade e estoura — e como o
    arquivo existe, todo request seguinte cai no mesmo caminho e devolve 500 até
    a escrita terminar. Foi exatamente isso em 18/08/2026 12:46–12:47: rajada de
    500 em /forecast durante o reaquecimento de 45min do `_refresh_loop`, que
    some sozinha quando o warm acaba. `os.replace` é atômico no POSIX, então o
    leitor sempre vê o arquivo velho inteiro OU o novo inteiro, nunca um pedaço.
    """
    tmp = cache_file.with_name(f".{cache_file.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        df.to_parquet(tmp, index=False)
        os.replace(tmp, cache_file)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


_bq_down_until = 0.0

# Stale-while-revalidate: quando o parquet vence, servimos o dado velho na hora
# e disparamos UM refresh em background (dedup por chave). Nenhum request de
# usuário paga o round-trip do BQ — só a primeiríssima população de uma chave.
_refresh_lock = threading.Lock()
_refreshing: set[str] = set()

_client = None
def client() -> bigquery.Client:
    global _client
    if _client is None:
        _client = bigquery.Client(project=PROJECT)
    return _client


def _query_raw(sql: str, cache_key: str | None = None, ttl: int = CACHE_TTL,
          force_refresh: bool = False) -> pd.DataFrame:
    """Run SQL and return DataFrame. Caches result as parquet when cache_key given.
    Se o BQ falhar (ex: view upstream quebrada), serve o cache mais próximo em
    disco em vez de derrubar o site — melhor dado velho que página 500."""
    global _bq_down_until
    cache_file = CACHE_DIR / f"{cache_key}.parquet" if cache_key else None
    now = time.time()

    # Cache em disco existe e não é force? Serve dele — sempre instantâneo.
    if cache_file and cache_file.exists() and not force_refresh:
        age = now - cache_file.stat().st_mtime
        if age < ttl:
            return pd.read_parquet(cache_file)          # fresco
        # Vencido → stale-while-revalidate: devolve o velho AGORA e atualiza em
        # background (se o BQ não estiver em cooldown). Usuário nunca espera.
        if now >= _bq_down_until:
            _spawn_refresh(sql, cache_key)
        return pd.read_parquet(cache_file)

    # Sem cache em disco. BQ caiu há pouco? tenta um irmão stale antes de falhar.
    if not force_refresh and now < _bq_down_until:
        stale = _stale_fallback(cache_key) if cache_key else None
        if stale is not None:
            return pd.read_parquet(stale)

    # 1ª população da chave OU force_refresh (botão ↻): consulta síncrona.
    try:
        df = client().query(sql).to_dataframe()
        _bq_down_until = 0.0  # BQ respondeu — re-arma
    except Exception as e:  # noqa: BLE001 — qualquer falha de BQ
        _bq_down_until = time.time() + BQ_COOLDOWN
        stale = _stale_fallback(cache_key) if cache_key else None
        if stale is not None:
            print(f"bq · {cache_key}: BQ falhou ({e}); servindo {stale.name}", flush=True)
            return pd.read_parquet(stale)
        raise
    if cache_file:
        _write_parquet_atomic(df, cache_file)
    return df


# ── Nomes de AE duplicados na FONTE ──────────────────────────────────────────
# O mesmo vendedor pode aparecer com dois nomes nas views do BQ, com a informação
# partida ao meio. Caso real (2026-07-30):
#   "João Pedro Vellardo"              user_id NULL, leader "No leader assigned",
#                                      mas carrega o pipeline e TODO o deal-level;
#   "João Pedro Vellardo Kouyomdjian"  user_id 83400553, leader e quota.
# Sem unificar, ele cai fora de ALLOWED_LEADERS e desaparece das telas
# deal-level, enquanto o nome do dropdown (o longo) não casa com deal nenhum e
# devolve tela vazia.
#
# A correção mora AQUI, no data layer, e não nos loaders do app: são 15 fontes
# com coluna de AE, e qualquer uma esquecida reintroduz o bug em silêncio. O
# parquet segue gravando o nome cru — a canonização acontece na LEITURA, então
# nenhuma chave de cache precisa de bump.
AE_NAME_ALIASES = {
    "João Pedro Vellardo": "João Pedro Vellardo Kouyomdjian",
    # "Diego Loures Peter" chega partido ao meio: a planilha da liderança (e o
    # Q3_ROSTER) usam o nome do meio, a base usa o sobrenome. Verificado em
    # 24/08/2026: "Diego Peter" tem 104 deals em int_adding_names_to_deals e
    # "Diego Loures" tem ZERO — é o único nome do roster que não aparece em deal
    # nenhum. Sem o alias, o roster criava uma linha sintética de zero pra ele e
    # os deals reais caíam fora (leader NULL na curada, fora do ALLOWED_LEADERS):
    # ele aparecia com R$ 0 no Overview tendo R$ 7.482 ganhos no Q3.
    "Diego Peter": "Diego Loures",
    # Grafia antiga do roster ("Thomas") → nome real no HubSpot ("Thomaz").
    "Thomas Talarico": "Thomaz Talarico",
    # México — a curada guarda a MESMA pessoa com duas grafias, e as duas têm
    # histórico. Verificado em 24/08/2026 na account_executives_performance:
    # "Viri Garza" (1 trimestre, US$ 2.880) e "Viridiana Garza" (3 trimestres);
    # "Adrián Hernández" com acento (3 trimestres) e "Adrian Hernandez" sem
    # (2 trimestres, US$ 34.624). Sem alias, o mesmo vendedor aparece como duas
    # linhas no ranking, cada uma com metade do resultado. O nome canônico é o
    # que a tabela de DEALS usa, senão o join com o funil quebra.
    "Viri Garza": "Viridiana Garza",
    "Adrián Hernández": "Adrian Hernandez",
}


def canon_ae_names(df: pd.DataFrame) -> pd.DataFrame:
    """Reescreve as variações de AE_NAME_ALIASES no nome canônico."""
    if df is not None and len(df) and "account_executive" in df.columns:
        df["account_executive"] = df["account_executive"].replace(AE_NAME_ALIASES)
    return df


SALES_BR = "Sales BR"


# ── Fonte RAW dos deals do HubSpot ───────────────────────────────────────────
# Toda tela que depende de ESTADO (estágio, close date, forecast category) lê o
# HubSpot cru, não a curada — o porquê está em `pipeline_deals_raw`. Até
# 14/09/2026 esse cru era `hubspot.hubspot_deals`, a tabela achatada legada.
#
# POR QUE MUDOU: a ingestão do HubSpot migrou pro Nekt e a legada PAROU de
# receber o sync completo. Medido em 14/09/2026:
#     hubspot.hubspot_deals        39.678 linhas · deal mais recente 11/09 17:12
#     hubspot_nekt.hubspot_deals   39.697 linhas · deal mais recente 14/09 09:06
# O dashboard servia o funil de SEXTA numa segunda de fim de trimestre: 36 deals
# com estágio errado e 17 com forecast category errada. Entre eles o Tate & Lyle
# (CP Kelco) Limeira/SP, R$ 37,2 mil já ganhos que não entravam no realizado, e
# 3 deals já perdidos (R$ 44 mil) ainda inflando o pipeline.
#
# O SQL não aponta pra tabela nenhuma direto, e sim pra view
# `pepeuhacker.hubspot_deals_live`, que entrega o shape da legada — dealId,
# dealstage, closedate, hs_manual_forecast_category. Duas razões: todo SQL que já
# dizia `h.dealstage` segue valendo sem reescrita, e a fonte passa a ser
# reversível por um `CREATE OR REPLACE VIEW`, sem deploy do app.
#
# 15/09/2026 — A VIEW DEIXOU DE ESCOLHER UMA FONTE E PASSOU A MESCLAR AS DUAS.
# Em cinco dias os papéis se inverteram duas vezes, e cada inversão serviu dado
# velho com cara de novo:
#     11/09  a legada parou o sync  -> a view foi apontada pra nekt crua
#     14/09  a nekt crua congelou às 10h56, quando o dbt do BI migrou pra
#            `hubspot_nekt.hubspot_deals_transformed` (outro nome, mesmo dado)
#            -> o dashboard voltou a servir funil de ontem, enquanto a legada,
#               com sync completo de volta (36 mil deals reescritos às 01h de
#               15/09), já estava 27h à frente
# O conserto do dia 15 não foi reapontar de novo: a view agora faz UNION das
# duas fontes vivas e fica com a linha mais recente de cada deal, por
# `hs_lastmodifieddate`. Quem estiver vivo ganha sozinho — na próxima vez que uma
# das duas parar, não há nada pra reapontar. O que a mescla recuperou naquele
# momento: 100 deals que a view nem listava, 217 com estágio errado (4 já ganhos
# e 65 já perdidos), 56 com forecast category errada e 189 com close date errado.
#
# O QUE ISTO NÃO CONSERTA: a cadeia curada do BI (`stg_hubspot__deals` →
# `int_adding_names_to_deals`) continua lendo a tabela legada, então `amount`,
# `account_executive` e `stage_label` seguem tão velhos quanto ela. Isso é
# conserto do time de BI. O que está no nosso alcance — e é o que muda de hora em
# hora no fim do trimestre — é estado e data, e esses saem do caminho congelado.
HS_DEALS_VIEW = "tractian-bi.pepeuhacker.hubspot_deals_live"
# Tabelas que a view MESCLA hoje (ver o comentário longo no DDL de
# `pepeuhacker.hubspot_deals_live`). Isto aqui serve SÓ ao gate do reaquecimento,
# que pergunta de metadado — 0 byte — se alguma delas foi gravada desde a última
# volta. O selo das telas não sai daqui: ver `hs_data_last_modified`.
HS_SOURCE_TABLES = (
    "tractian-bi.hubspot.hubspot_deals",
    "tractian-bi.hubspot_nekt.hubspot_deals_transformed",
)


def hs_source_last_modified() -> float:
    """Epoch (segundos) da gravação mais recente em QUALQUER fonte da view.

    Gate do reaquecimento, e só. Lê METADADO (`__TABLES__`): custa 0 byte e pode
    ser chamado de minuto em minuto sem pesar na conta.

    NÃO serve de selo de frescor, e essa distinção custou caro: o
    `last_modified_time` da legada sobe com INSERT avulso de aplicação
    (`gtm-applications@` e afins) mesmo com o sync do HubSpot morto — foi assim
    que ela passou três dias parada, de 11 a 14/09/2026, com cara de tabela
    recém-escrita. Como gate, uma gravação dessas custa um reaquecimento à toa;
    como selo, mentiria para quem está fechando trimestre.

    Devolve 0.0 se falhar: frescor é informação de apoio e não pode derrubar
    tela nem travar o refresher."""
    partes = []
    for full in HS_SOURCE_TABLES:
        proj, ds, name = full.split(".")
        partes.append(f"SELECT last_modified_time AS t "
                      f"FROM `{proj}.{ds}.__TABLES__` WHERE table_id = '{name}'")
    sql = f"SELECT MAX(t) FROM ({' UNION ALL '.join(partes)})"
    try:
        rows = list(client().query(sql).result())
        val = rows[0][0] if rows else None
        return float(val) / 1000.0 if val else 0.0
    except Exception as e:  # noqa: BLE001 — ver docstring
        print(f"bq · hs_source_last_modified falhou ({e})", flush=True)
        return 0.0


def hs_data_last_modified() -> float:
    """Epoch (segundos) do dado mais novo DENTRO da view — MAX(hs_lastmodifieddate).

    É este o número do selo das telas. `hs_lastmodifieddate` é campo do próprio
    HubSpot: quando o sync para, ele para junto, e a tela passa a dizer "dado de
    27h atrás" em vez de repetir a hora em que alguma aplicação encostou na
    tabela. Metadado de gravação não sabe fazer isso (ver acima).

    Custa ~1,5 MB por chamada (uma coluna, ~40 mil linhas) — abaixo do mínimo de
    cobrança do BigQuery. É chamado no máximo de 15 em 15 min, pelo refresher e
    pela sonda de dev; não chame dentro de um request.

    Devolve 0.0 se falhar, pelo mesmo motivo de `hs_source_last_modified`."""
    sql = f"SELECT UNIX_SECONDS(MAX(hs_lastmodifieddate)) FROM `{HS_DEALS_VIEW}`"
    try:
        rows = list(client().query(sql).result())
        val = rows[0][0] if rows else None
        return float(val) if val else 0.0
    except Exception as e:  # noqa: BLE001 — ver docstring
        print(f"bq · hs_data_last_modified falhou ({e})", flush=True)
        return 0.0



# Estágios de FECHAMENTO por pipeline. O México é outro funil no HubSpot e não
# compartilha id nenhum com o Brasil: won lá é '13023548' e lost é '8352516'.
# Sem isto, uma query escrita pro BR roda no MX sem erro e devolve "nenhum deal
# ganho, todos abertos" — foi exatamente o que aconteceu no 1º teste do México.
CLOSED_STAGES = {
    "Sales BR": {"won": "closedwon", "lost": "closedlost"},
    "Sales MX": {"won": "13023548", "lost": "8352516"},
}


def _apply_pipeline(sql: str, pipeline: str) -> str:
    """Traduz a SQL do vocabulario do Brasil pro do pipeline alvo.

    Troca o funil (`pipeline_label`) E TODOS os ids de estagio que aparecem em
    WHERE/IN — won/lost inclusive. As duas coisas juntas porque esquecer
    qualquer uma e SILENCIOSO: a query roda, nao da erro, e devolve resultado
    errado.

    O segundo item foi descoberto tarde: `pipeline_stalled` filtra
    `stage_curr IN ('contractsent','93878633',...)`, que sao ids do BR. No
    Mexico nada casava e a tela de Inatividade voltava VAZIA — 0 linhas contra
    1.024 do Brasil, sem uma linha de erro no log.

    A traducao e so na IDA (o que a SQL filtra). Na VOLTA, `normalize_stages`
    faz o inverso e devolve ids canonicos do BR pro app inteiro."""
    if pipeline == SALES_BR:
        return sql
    sql = sql.replace("'Sales BR'", f"'{pipeline}'")
    para = _SQL_STAGE_MAP.get(pipeline)
    if not para:
        return sql
    # Passe UNICO sobre literais entre aspas: substituir em cadeia arriscaria
    # traduzir duas vezes um id que ja virou outro.
    return re.sub(r"'([A-Za-z0-9_]+)'",
                  lambda m: f"'{para.get(m.group(1), m.group(1))}'", sql)


def _pk(key, pipeline):
    """Sufixo de pipeline na chave de cache. O Brasil NAO recebe sufixo, de
    proposito: assim todo parquet ja gravado continua valido e o BR nao perde
    cache nem muda de comportamento por causa do Mexico."""
    if not key or pipeline == SALES_BR:
        return key
    return f"{key}__{pipeline.lower().replace(' ', '_')}"


# Colunas de dinheiro que as queries compartilhadas devolvem. Fora do Brasil elas
# vêm na moeda do deal (USD, MXN ou BRL misturados), então precisam ser
# convertidas ANTES de qualquer soma — senão peso vira dólar na conta.
_MONEY_COLS = {"amount", "amount_prev", "amount_curr", "amt_from", "amt_to",
               "amount_at", "amt", "weighted"}
_FX_CACHE: dict = {}


def usd_factor_by_deal(pipeline: str) -> dict:
    """deal_id -> fator que converte o `amount` daquele deal em USD.

    Usa a cotação vigente NA DATA do deal (`currency_conversion_rates`), com
    fallback pra a mais antiga e, por fim, 1.0. Um mapa só por pipeline, cacheado
    em memória: são 7 mil linhas no MX e evita repetir o join em cada query."""
    if pipeline in _FX_CACHE:
        return _FX_CACHE[pipeline]
    sql = f"""
    WITH fx AS (
      SELECT currency, conversion_usd, PARSE_DATE('%Y-%m-%d', valid_date) AS valid_date
      FROM `tractian-bi.dbt_analytics_dev.currency_conversion_rates`
      WHERE conversion_usd IS NOT NULL
    )
    SELECT d.deal_id,
      COALESCE((SELECT fx.conversion_usd FROM fx
                WHERE fx.currency = UPPER(TRIM(COALESCE(d.deal_currency,'USD')))
                  AND fx.valid_date <= COALESCE(d.close_date, d.created_date, CURRENT_DATE())
                ORDER BY fx.valid_date DESC LIMIT 1),
               (SELECT fx.conversion_usd FROM fx
                WHERE fx.currency = UPPER(TRIM(COALESCE(d.deal_currency,'USD')))
                ORDER BY fx.valid_date ASC LIMIT 1), 1.0) AS f
    FROM `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
    WHERE d.pipeline_label = '{pipeline}'
    """
    df = _query_raw(sql, cache_key=f"usd_factor__{pipeline.lower().replace(' ','_')}",
                    ttl=86400)
    m = {} if df is None or not len(df) else dict(
        zip(df["deal_id"].astype(str), pd.to_numeric(df["f"], errors="coerce").fillna(1.0)))
    _FX_CACHE[pipeline] = m
    return m


def to_usd(df, pipeline: str):
    """Converte as colunas de dinheiro do frame pra USD. No-op no Brasil, que já
    é uma moeda só e cujo toggle MRR/ARR faz a conversão na exibição."""
    if pipeline == SALES_BR or df is None or not len(df):
        return df
    cols = _MONEY_COLS & set(df.columns)
    if not cols or "deal_id" not in df.columns:
        return df
    fac = df["deal_id"].astype(str).map(usd_factor_by_deal(pipeline)).fillna(1.0)
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0) * fac
    return df


# ------------------------------------------------------------------
# IDs de estágio do MX -> IDs canônicos do BR
# ------------------------------------------------------------------
# O funil mexicano tem a MESMA estrutura do brasileiro (Stage 1..6, Won, Lost,
# No Show) com ids diferentes. Traduzir na volta da query é o que faz TODO o
# código de estágio do app — ordem do funil, grupos early/late, rótulos,
# conversão — funcionar no México sem uma linha de exceção. A alternativa era
# espalhar `if _is_mx()` por seis lookups diferentes e esquecer de um.
STAGE_ID_MAP = {
    "Sales MX": {
        "8352441":  "appointmentscheduled",   # Stage 1 - Scheduled
        "8352512":  "contractsent",           # Stage 2 - Qualifying
        "94159788": "93878633",               # Stage 3 - Scoping
        "8352513":  "decisionmakerboughtin",  # Stage 4 - Negotiation
        "89673855": "87188634",               # Stage 5 - Decision
        "66011247": "65917881",               # Stage 6 - Procurement
        "13023548": "closedwon",
        "8352516":  "closedlost",
        "61414206": "53862044",               # No Show
    },
}
# TODA coluna que carrega id de estagio. `from_stage`/`to_stage` (com o
# substantivo na frente) faltavam aqui, e sao justamente as que funnel_transitions
# devolve: sem elas o Funil do Mexico ficava com a movimentacao inteira ZERADA —
# as setas mostravam 0 em todos os estagios e em todos os AEs, sem erro nenhum,
# porque os ids voltavam em vocabulario mexicano e o app comparava com os do BR.
_STAGE_COLS = {"dealstage", "stage", "stage_from", "stage_to", "stage_at",
               "stage_prev", "stage_curr", "dealstage_id",
               "from_stage", "to_stage"}

# Inverso do STAGE_ID_MAP: canonico do BR -> id do pipeline alvo. Usado so pra
# reescrever a SQL na IDA; o app segue falando o vocabulario do Brasil.
_SQL_STAGE_MAP = {pl: {v: k for k, v in m.items()} for pl, m in STAGE_ID_MAP.items()}


def normalize_stages(df, pipeline: str):
    """Reescreve ids de estágio do pipeline pro vocabulário canônico do BR."""
    m = STAGE_ID_MAP.get(pipeline)
    if not m or df is None or not len(df):
        return df
    for c in _STAGE_COLS & set(df.columns):
        df[c] = df[c].map(lambda v: m.get(v, v))
    return df


def query(sql: str, cache_key: str | None = None, ttl: int = CACHE_TTL,
          force_refresh: bool = False, pipeline: str = SALES_BR) -> pd.DataFrame:
    """`_query_raw` + canonização de nome de AE. Wrapper (e não edição dos 6
    pontos de return de `_query_raw`) pra cobrir também os caminhos de cache
    stale e de fallback quando o BQ está fora."""
    return normalize_stages(
        to_usd(canon_ae_names(_query_raw(sql, cache_key=cache_key, ttl=ttl,
                                         force_refresh=force_refresh)), pipeline),
        pipeline)


def _spawn_refresh(sql: str, cache_key: str | None) -> None:
    """Refresca o parquet de uma chave em background, sem bloquear o request.
    Dedup por chave: refreshes concorrentes da mesma chave viram um só."""
    if not cache_key:
        return
    with _refresh_lock:
        if cache_key in _refreshing:
            return
        _refreshing.add(cache_key)

    def _work():
        global _bq_down_until
        try:
            df = client().query(sql).to_dataframe()
            _bq_down_until = 0.0
            _write_parquet_atomic(df, CACHE_DIR / f"{cache_key}.parquet")
        except Exception as e:  # noqa: BLE001 — best-effort; mantém o cache velho
            _bq_down_until = time.time() + BQ_COOLDOWN
            print(f"bq · refresh bg {cache_key} falhou ({e}); mantém cache velho", flush=True)
        finally:
            with _refresh_lock:
                _refreshing.discard(cache_key)

    threading.Thread(target=_work, name=f"bq-refresh-{cache_key}", daemon=True).start()


def _stale_fallback(cache_key: str) -> Path | None:
    """Melhor parquet em cache quando o BQ está fora. Tenta a chave exata
    (qualquer idade); senão, pra chaves com janela de data
    (ex: pipe_wow_2026-05-24..._2026-05-31...), pega o snapshot mais recente do
    mesmo prefixo. Devolve None se não há nada — aí o erro propaga normal."""
    exact = CACHE_DIR / f"{cache_key}.parquet"
    if exact.exists():
        return exact
    prefix = re.split(r"_\d{4}-\d{2}-\d{2}", cache_key)[0]  # corta no 1º token de data
    if prefix and prefix != cache_key:
        sibs = sorted(CACHE_DIR.glob(f"{prefix}_*.parquet"),
                      key=lambda p: p.stat().st_mtime, reverse=True)
        if sibs:
            return sibs[0]
    return None


def ae_performance(force_refresh: bool = False) -> pd.DataFrame:
    """Main curated table — 1 row per AE per quarter, FY25 onward.
    Filters out rows with null region (edge-case churn aggregates)."""
    sql = """
    SELECT *
    FROM `tractian-bi.dbt_analytics_dev.account_executives_performance`
    WHERE quarter >= DATE '2024-01-01'
      AND region IS NOT NULL
    ORDER BY quarter DESC, new_mrr_in_brl DESC
    """
    return query(sql, cache_key="ae_performance", force_refresh=force_refresh)


def quota_history(force_refresh: bool = False) -> pd.DataFrame:
    """Quota TRIMESTRAL do BR direto do BQ — base do Sales Overtime.

    Fonte: `tractian_sales.sales_rep_monthly_quota` (quota mensal por rep). É a
    meta OFICIAL e COMPLETA do time: inclui todo mundo que carrega meta (AEs,
    gestores, etc.), não só os AEs do roster curado. Somando os reps chegamos à
    meta do time — por isso NÃO usamos `account_executives_performance`, que só
    enxerga role='AE' e sub-conta a meta (ex.: Q3/26 R$ 2,1M lá vs R$ 4,99M aqui).

    `sales_region_monthly_quota` (meta por região) foi descartada: está congelada
    em 2023-01..2023-08.

    `account_executive` vem do join por id e é NULL pra quem não tem deal na
    curated (gestores/CS) — esses entram no TOTAL, mas não em filtros por AE.
    Cobertura: a partir de 2023-Q3 (antes disso não há meta registrada)."""
    sql = """
    WITH ae AS (
      SELECT DISTINCT CAST(account_executive_id AS STRING) AS id, account_executive
      FROM `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals`
      WHERE account_executive_id IS NOT NULL AND account_executive IS NOT NULL
    )
    SELECT
      DATE_TRUNC(q.month, QUARTER)                        AS quarter,
      CAST(q.user_id AS STRING)                           AS user_id,
      ae.account_executive                                AS account_executive,
      SUM(COALESCE(q.quota_in_brl, 0))                    AS quota_in_brl,
      SUM(COALESCE(q.new_logos_quota, 0))                 AS new_logos_quota
    FROM `tractian-bi.tractian_sales.sales_rep_monthly_quota` q
    LEFT JOIN ae ON ae.id = CAST(q.user_id AS STRING)
    WHERE LOWER(q.region) = 'br'
    GROUP BY quarter, user_id, account_executive
    ORDER BY quarter
    """
    return query(sql, cache_key="quota_history_v2", force_refresh=force_refresh)


def funnel_stage_entries(pipeline: str = SALES_BR, force_refresh: bool = False) -> pd.DataFrame:
    """Primeira entrada de cada deal em cada estágio do funil Sales BR.

    Base da CONVERSÃO DE FUNIL ao longo do tempo: com a data em que o deal pisou
    pela 1ª vez em cada estágio dá pra montar coortes — "dos deals que entraram
    no estágio A no trimestre X, quantos chegaram ao estágio B". Reconstruído do
    array `dealstage` de hubspot_deals_history (mesma fonte do stalled/WoW).

    Inclui `closedlost`: sem ele não dá pra separar "deal perdido" de "deal ainda
    aberto", e a conversão fica subestimada nos trimestres recentes (a coorte
    ainda em andamento entra no denominador como se tivesse falhado).

    Uma linha por (deal, estágio). ~41k linhas / ~16k deals — cabe no parquet."""
    sql = """
    SELECT
      CAST(h.dealId AS STRING)  AS deal_id,
      s.value                   AS stage,
      MIN(s.timestamp)          AS first_at,
      ANY_VALUE(d.account_executive) AS account_executive,
      ANY_VALUE(d.deal_type)         AS deal_type,
      ANY_VALUE(d.created_date)      AS created_date,
      ANY_VALUE(d.deal_name)         AS deal_name,
      ANY_VALUE(COALESCE(SAFE_CAST(d.amount AS FLOAT64), 0)) AS amount
    FROM `tractian-bi.pepeuhacker.hubspot_deals_history_live` h,
         UNNEST(h.dealstage) s
    JOIN `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
      ON CAST(d.deal_id AS INT64) = h.dealId
    WHERE d.pipeline_label = 'Sales BR'
      AND s.value IN ('appointmentscheduled','contractsent','93878633',
                      'decisionmakerboughtin','87188634','65917881',
                      'closedwon','closedlost')
    GROUP BY deal_id, stage
    """
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk("funnel_stage_entries_v5", pipeline), force_refresh=force_refresh)


def demand_generation(date_from: str, date_to: str, force_refresh: bool = False) -> pd.DataFrame:
    """Demand generated per AE (as qualification owner), 4 buckets:
      - auto_acq: AE qualificou ele mesmo, tipo Aquisição
      - auto_exp: AE qualificou ele mesmo, tipo Expansão
      - rcv_acq:  SDR qualificou (≠ AE), tipo Aquisição
      - rcv_exp:  SDR qualificou (≠ AE), tipo Expansão
    Source: int_adding_names_to_deals (tem account_executive_id separado do customer_owner_id=CS)
    Joins com account_executives_performance pra pegar leader/region/role filter."""
    sql = f"""
    WITH
    ae_meta AS (
      SELECT account_executive, leader, region, role
      FROM `tractian-bi.dbt_analytics_dev.account_executives_performance`
      WHERE region IS NOT NULL AND role = 'AE'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY account_executive ORDER BY quarter DESC) = 1
    ),
    deals AS (
      SELECT
        d.account_executive,
        d.account_executive_id,
        d.sdr_id,
        CASE
          WHEN d.sdr_id IS NULL                             THEN 'auto'
          WHEN d.sdr_id = d.account_executive_id            THEN 'auto'
          ELSE 'received'
        END AS source,
        CASE
          WHEN d.deal_type = 'Aquisition'     THEN 'acq'
          WHEN d.deal_type = 'Up & cross sell' THEN 'exp'
          ELSE 'other'
        END AS kind,
        -- Canal vem da propriedade `motion` do HubSpot. `deal_source_agg` (que
        -- deriva de `source`) foi aposentado como classificador em 02/09/2026:
        -- o time parou de gravar source='Outbound' em 17/06/2026 e migrou pra
        -- Cold Call / Cold Email / Unattributed, valores que a regra do dbt lê
        -- como Inbound. Em ago/2026 isso zerava o outbound recebido (0 contra
        -- 185 reais). Quando motion está vazia (deals de 2024, 71% sem valor)
        -- cai de volta no deal_source_agg pra não perder o histórico.
        CASE
          WHEN d.motion = 'Outbound' THEN 'outbound'
          WHEN d.motion IS NOT NULL  THEN 'inbound'
          WHEN d.deal_source_agg = 'Outbound' THEN 'outbound'
          ELSE 'inbound'
        END AS channel
      FROM `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
      WHERE d.created_date BETWEEN DATE '{date_from}' AND DATE '{date_to}'
        AND d.created_date <= CURRENT_DATE()      -- sem data futura (sanity)
        AND d.account_executive IS NOT NULL
    )
    SELECT
      d.account_executive, ae.leader, ae.region,
      COUNTIF(d.source = 'auto'     AND d.kind = 'acq')                            AS auto_acq,
      COUNTIF(d.source = 'auto'     AND d.kind = 'exp')                            AS auto_exp,
      -- Recebidas: split por canal só pra Acquisition; Expansão é bucket separado.
      COUNTIF(d.source = 'received' AND d.kind = 'acq' AND d.channel = 'outbound') AS rcv_outbound,
      COUNTIF(d.source = 'received' AND d.kind = 'acq' AND d.channel = 'inbound')  AS rcv_inbound,
      COUNTIF(d.source = 'received' AND d.kind = 'exp')                            AS rcv_exp,
      COUNTIF(d.kind IN ('acq','exp'))                                              AS total_generated
    FROM deals d
    LEFT JOIN ae_meta ae USING (account_executive)
    GROUP BY d.account_executive, ae.leader, ae.region
    HAVING total_generated > 0
    ORDER BY total_generated DESC
    """
    return query(sql, cache_key=f"demand_gen_{date_from}_{date_to}", force_refresh=force_refresh)


def demand_generation_grain(date_from: str, date_to: str, grain: str = "month",
                              force_refresh: bool = False) -> pd.DataFrame:
    """Time series de demanda gerada agrupada por grão (week/month/quarter)
    com breakdown por AE — pra permitir filtrar por líder/AE no app.
    Colunas: period, account_executive, leader, region, source, kind, n."""
    grain_sql = {
        "week":    "WEEK(MONDAY)",
        "month":   "MONTH",
        "quarter": "QUARTER",
    }.get(grain, "MONTH")

    sql = f"""
    WITH ae_meta AS (
      SELECT account_executive, leader, region
      FROM `tractian-bi.dbt_analytics_dev.account_executives_performance`
      WHERE region IS NOT NULL AND role = 'AE'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY account_executive ORDER BY quarter DESC) = 1
    )
    SELECT
      DATE_TRUNC(d.created_date, {grain_sql}) AS period,
      d.account_executive,
      ae.leader,
      ae.region,
      CASE
        WHEN d.sdr_id IS NULL                  THEN 'auto'
        WHEN d.sdr_id = d.account_executive_id THEN 'auto'
        ELSE 'received'
      END AS source,
      -- Auto: classifica por kind (acq/exp). Recebido: classifica por canal (in/out)
      -- exceto Expansão que é bucket próprio (não cai em nenhuma source).
      CASE
        WHEN (d.sdr_id IS NULL OR d.sdr_id = d.account_executive_id) THEN
          CASE WHEN d.deal_type = 'Aquisition'      THEN 'acq'
               WHEN d.deal_type = 'Up & cross sell' THEN 'exp'
               ELSE 'other' END
        ELSE
          CASE WHEN d.deal_type = 'Up & cross sell' THEN 'exp'
               WHEN d.deal_type = 'Aquisition' AND COALESCE(d.motion, d.deal_source_agg)='Outbound' THEN 'outbound'
               WHEN d.deal_type = 'Aquisition'                                                      THEN 'inbound'
               ELSE 'other' END
      END AS kind,
      COUNT(*) AS n
    FROM `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
    LEFT JOIN ae_meta ae USING (account_executive)
    WHERE d.created_date BETWEEN DATE '{date_from}' AND DATE '{date_to}'
      AND d.created_date <= CURRENT_DATE()
      AND d.account_executive IS NOT NULL
    GROUP BY period, account_executive, leader, region, source, kind
    HAVING kind != 'other'
    ORDER BY period
    """
    return query(sql, cache_key=f"demand_grain_{grain}_{date_from}_{date_to}",
                 force_refresh=force_refresh)


# Backwards-compat alias
def demand_generation_monthly(date_from: str, date_to: str, force_refresh: bool = False) -> pd.DataFrame:
    df = demand_generation_grain(date_from, date_to, "month", force_refresh)
    return df.rename(columns={"period": "month"}) if "period" in df.columns else df


def demand_deals_raw(pipeline: str = SALES_BR, force_refresh: bool = False) -> pd.DataFrame:
    """Raw deal-level data for /demand and /leaders/demand. Single broad query
    cached for 1h — todo slicing por data/grão/líder é feito em pandas no app.
    Substitui demand_generation() + demand_generation_grain() pra eliminar o
    roundtrip BQ por cada combinação de date_from/date_to/grain (era ~15-25s
    cada combo cold)."""
    sql = """
    SELECT
      d.account_executive,
      d.account_executive_id,
      d.sdr_id,
      d.created_date,
      d.deal_type,
      d.deal_source_agg,
      -- Propriedade `motion` do HubSpot (Inbound/Outbound/Partner/Unattributed).
      -- É ela que classifica canal desde 02/09/2026 — ver _get_demand_deals().
      d.motion,
      d.deal_id,
      d.deal_name,
      COALESCE(SAFE_CAST(d.amount AS FLOAT64), 0) AS amount,
      d.deal_currency,
      d.stage_label,
      d.sdr,
      -- Estágio AO VIVO (view que mescla as fontes do HubSpot). A curated só
      -- enxerga a legada, parada desde 11/10/2026.
      h.dealstage,
      -- Chegou ao estágio 3 (Scoping)? Conta quem entrou em Scoping OU em
      -- qualquer etapa depois (deal que pulou de Qualifying pra Negotiation
      -- também passou do 3) e quem está nele ou além AGORA — este último pega
      -- o que avançou depois que as datas de entrada da curated pararam.
      -- Os ids são do BR; `_apply_pipeline` traduz pro MX.
      (COALESCE(d.hs_date_entered_project_scoping, d.hs_date_entered_negotiation,
                d.hs_date_entered_consensus, d.hs_date_entered_procurement) IS NOT NULL
       OR h.dealstage IN ('93878633', 'decisionmakerboughtin', '87188634',
                          '65917881', 'closedwon')) AS reached_scoping,
      -- 1ª data em etapa 3+ (as datas de entrada são DATE; MIN ignora NULL).
      (SELECT MIN(x) FROM UNNEST([d.hs_date_entered_project_scoping,
                                  d.hs_date_entered_negotiation,
                                  d.hs_date_entered_consensus,
                                  d.hs_date_entered_procurement]) x) AS scoping_reached_date
    FROM `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
    LEFT JOIN `tractian-bi.pepeuhacker.hubspot_deals_live` h
      ON h.dealId = d.deal_id
    -- Filtro de funil que faltava. Esta era a UNICA query deal-level do app sem
    -- ele, e por isso o frame de demanda atravessava paises: um drill do Brasil
    -- com `name` de vendedor mexicano devolvia os deals do Mexico rotulados em
    -- R$. Achado por auditoria em 24/08/2026 — nao dava erro nenhum.
    WHERE d.pipeline_label = 'Sales BR'
      AND d.created_date >= DATE '2024-01-01'
      AND d.created_date <= CURRENT_DATE()
      AND d.account_executive IS NOT NULL
    """
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk("demand_deals_raw_v6", pipeline),
                 force_refresh=force_refresh)


def motion_history(force_refresh: bool = False) -> pd.DataFrame:
    """Histórico da propriedade `motion` por deal, reconstruído das cópias
    DIÁRIAS que o BI grava em `materialized_data_source`.

    Por que daqui e não da API: `hubspot_deals_history` versiona só 7
    propriedades e motion não é uma delas. Mas o BI materializa a curated
    `int_adding_names_to_deals` todo dia (`int_adding_names_to_deals<YYYYMMDD>`),
    e a coluna `motion` entrou nessas cópias em 16/04/2026 — 139 dias sem furo
    até 01/09/2026. Comparando snapshots consecutivos dá pra ver cada troca de
    motion, que é o que a API só entregaria via `propertiesWithHistory`.

    LIMITE: deal criado antes de 16/04/2026 não tem série anterior a essa data,
    então uma troca feita antes disso é invisível aqui. O app trata esses deals
    como "sem mudança conhecida" — ver `motion_hist_coverage_from`.

    Wildcard + _TABLE_SUFFIX em vez de UNION ALL das 139 tabelas: mesmo
    resultado (conferido linha a linha), 4s em vez de dezenas.

    Uma linha por deal que teve pelo menos uma troca de motion. Colunas:
    deal_id, motion_first, motion_last, n_changes, motion_changed_at."""
    sql = f"""
    WITH hist AS (
      SELECT PARSE_DATE('%Y%m%d', CONCAT('2', _TABLE_SUFFIX)) AS snap,
             CAST(deal_id AS STRING) AS deal_id,
             motion
      FROM `tractian-bi.materialized_data_source.int_adding_names_to_deals2*`
      WHERE _TABLE_SUFFIX >= '{MOTION_HIST_SUFFIX_FROM}'
        AND pipeline_label = 'Sales BR'
    ),
    seq AS (
      SELECT deal_id, snap, motion,
             LAG(motion) OVER (PARTITION BY deal_id ORDER BY snap) AS prev,
             ROW_NUMBER() OVER (PARTITION BY deal_id ORDER BY snap) AS rn
      FROM hist
    ),
    -- rn=1 é a 1ª aparição do deal na série, não uma troca. E (vazio)->X é o
    -- preenchimento inicial do campo: também não conta como mudança de canal.
    chg AS (
      SELECT deal_id, snap, prev, motion
      FROM seq
      WHERE rn > 1 AND prev IS DISTINCT FROM motion
        AND prev IS NOT NULL AND motion IS NOT NULL
    ),
    -- Primeiro valor não-nulo que o deal teve, pra saber de onde ele saiu.
    first_val AS (
      SELECT deal_id, motion AS motion_first
      FROM seq
      WHERE motion IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (PARTITION BY deal_id ORDER BY snap) = 1
    ),
    last_val AS (
      SELECT deal_id, motion AS motion_last
      FROM seq
      WHERE motion IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (PARTITION BY deal_id ORDER BY snap DESC) = 1
    )
    SELECT
      c.deal_id,
      ANY_VALUE(f.motion_first) AS motion_first,
      ANY_VALUE(l.motion_last)  AS motion_last,
      COUNT(*)                  AS n_changes,
      MAX(c.snap)               AS motion_changed_at
    FROM chg c
    LEFT JOIN first_val f USING (deal_id)
    LEFT JOIN last_val  l USING (deal_id)
    GROUP BY c.deal_id
    """
    return query(sql, cache_key="motion_history_v1", force_refresh=force_refresh)


# A coluna `motion` só entra nas cópias diárias do BI em 16/04/2026 — antes
# disso não há como saber se o campo mudou. Sufixo no formato que o wildcard
# `int_adding_names_to_deals2*` espera (o "2" inicial já está no prefixo).
MOTION_HIST_SUFFIX_FROM = "0260416"
MOTION_HIST_FROM = "2026-04-16"


def visits_raw(force_refresh: bool = False) -> pd.DataFrame:
    """Visitas (objeto HubSpot `hubspot_visits`) por vendedor, nível-visita.
    Categoria por LOCAL da visita:
      - 'ia_center' = pipeline 890777614 (cliente vai ao IA Center da Tractian)
      - 'field'     = demais visitas do AE (vendedor no cliente / campo)
    O AE é creditado quando é o DONO da visita (`hubspot_owner_id`) OU quando é
    PARTICIPANTE (e-mail dele em `collaborators_in_visit`, lista separada por ';').
    Dono → AE via account_executive_id; participante → AE via e-mail do owner
    (hubspot_owner_id → email → account_executive_id). Uma visita pode creditar mais
    de um AE (cada participante-AE conta a visita); dedup por (AE, visita) evita
    contar 2× quando o AE é dono E participante. Muitas visitas têm dono nulo mas
    têm colaboradores — o caminho de participante recupera essas. Cache 1h; slicing
    por período/líder feito em pandas no app."""
    sql = """
    WITH owner_ae AS (
      SELECT account_executive_id, ANY_VALUE(account_executive) AS account_executive
      FROM `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals`
      WHERE account_executive_id IS NOT NULL AND account_executive IS NOT NULL
      GROUP BY account_executive_id
    ),
    email_ae AS (  -- e-mail do owner → AE (pra casar participantes por e-mail)
      SELECT LOWER(o.email) AS email, ae.account_executive
      FROM `tractian-bi.hubspot.hubspot_owner_id` o
      JOIN owner_ae ae ON CAST(o.ownerId AS STRING) = ae.account_executive_id
      WHERE o.email IS NOT NULL
    ),
    -- 12/10/2026: a legada `hubspot.hubspot_visits` parou de sincronizar em
    -- 02/10 (55 visitas de 01 a 11/10 contra 93 na nekt). Mescla as duas, com
    -- preferência pela nekt (mesmo id: 411/412 visitas de set/26 iguais em data e
    -- pipeline; a legada só completa o que faltar). Mesma lógica da
    -- hubspot_deals_live: quem estiver vivo ganha sozinho.
    v_src AS (
      SELECT visit_id AS hs_object_id, SAFE_CAST(start_date AS DATE) AS start_date,
             pipeline AS hs_pipeline, hubspot_owner_id, collaborators_in_visit,
             1 AS prioridade
      FROM `tractian-bi.hubspot_nekt.analytics_canonical_hubspot_visits`
      WHERE _sdc_deleted_at IS NULL
      UNION ALL
      SELECT hs_object_id, start_date, hs_pipeline, hubspot_owner_id,
             collaborators_in_visit, 2
      FROM `tractian-bi.hubspot.hubspot_visits`
    ),
    v AS (
      SELECT
        v.hs_object_id,
        v.start_date,
        CASE WHEN v.hs_pipeline = '890777614' THEN 'ia_center' ELSE 'field' END AS category,
        v.hubspot_owner_id,
        v.collaborators_in_visit
      FROM v_src v
      WHERE v.hs_object_id IS NOT NULL
        AND v.start_date >= DATE '2024-01-01'
        AND v.start_date <= CURRENT_DATE()
      QUALIFY ROW_NUMBER() OVER (PARTITION BY v.hs_object_id ORDER BY v.prioridade) = 1
    ),
    by_owner AS (  -- creditado por ser DONO da visita
      SELECT ae.account_executive, v.hs_object_id, v.start_date, v.category
      FROM v JOIN owner_ae ae ON v.hubspot_owner_id = ae.account_executive_id
    ),
    by_participant AS (  -- creditado por ser PARTICIPANTE (e-mail em collaborators)
      SELECT ea.account_executive, v.hs_object_id, v.start_date, v.category
      FROM v,
           UNNEST(SPLIT(LOWER(v.collaborators_in_visit), ';')) AS collab_email
      JOIN email_ae ea ON TRIM(collab_email) = ea.email
      WHERE v.collaborators_in_visit IS NOT NULL AND v.collaborators_in_visit != ''
    )
    SELECT account_executive, start_date, category
    FROM (
      SELECT DISTINCT account_executive, hs_object_id, start_date, category
      FROM (SELECT * FROM by_owner UNION ALL SELECT * FROM by_participant)
    )
    """
    return query(sql, cache_key="visits_raw_v3", force_refresh=force_refresh)


def pipeline_total_raw(pipeline: str = SALES_BR, force_refresh: bool = False) -> pd.DataFrame:
    """Pipeline ABERTO do Sales BR com os eixos de corte da aba Pipeline Total:
    estágio, deal type, source, product e forecast category — tudo numa linha
    por deal, com o amount.

    Por que não reusar as queries existentes: `pipeline_deals_raw` tem stage e
    is_open mas não tem forecast category nem source; `forecast_deals_raw` tem
    forecast category mas recorta os estágios 1-6 + won e não traz source. Aqui
    o recorte é "todo o dinheiro que está no funil", então a query é ampla e
    quem filtra é o pandas no app (cache de 1h, mesma estratégia do resto).

    `is_open` vem do RAW do HubSpot (`pepeuhacker.hubspot_deals_live`), não da curada,
    pelo mesmo motivo documentado em `pipeline_deals_raw`: a curada devolve
    close_date NULL pra closedlost e confunde "aberto" com "perdido sem data".
    """
    sql = """
    SELECT
      d.account_executive,
      d.deal_id,
      d.deal_name,
      SAFE_CAST(d.amount AS FLOAT64) AS amount,
      d.stage_label,
      h.dealstage,
      d.deal_type,
      d.deal_source_agg,
      -- Propriedade "Product" do HubSpot (`product_of_deal`). MULTI-SELECT: vem
      -- como 'Smart Trac;TracOS' numa string só, então quem consome tem que
      -- separar por ';' em vez de comparar a string inteira.
      d.product_of_deal AS product,
      h.hs_manual_forecast_category AS forecast_category,
      d.created_date,
      DATE(h.closedate) AS close_date,
      -- Customer Owner (CS) do deal. Só a PRESENÇA importa pro filtro da aba.
      d.customer_owner_id,
      (h.dealstage IS NULL OR h.dealstage NOT IN ('closedwon','closedlost')) AS is_open
    FROM `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
    LEFT JOIN `tractian-bi.pepeuhacker.hubspot_deals_live` h
      ON CAST(h.dealId AS STRING) = d.deal_id
    WHERE d.pipeline_label = 'Sales BR'
      AND d.account_executive IS NOT NULL
    """
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk("pipeline_total_raw_v4", pipeline), force_refresh=force_refresh)


def pipeline_deals_raw(pipeline: str = SALES_BR, force_refresh: bool = False) -> pd.DataFrame:
    """Todos os deals do funil Sales BR. Verdade sobre estado e close date vem
    do RAW HubSpot (`pepeuhacker.hubspot_deals_live`), não da curated — porque a curated
    `close_date` retorna NULL pra deals que estão `closedlost` no HubSpot, o
    que confunde "deal aberto" com "deal lost sem data". Usado:
      - is_open  = h.dealstage NOT IN (closedwon, closedlost)
      - hs_closedate = h.closedate (Close Date do HubSpot)
    `account_executive`, `stage_label`, `amount` continuam vindo da curated."""
    sql = """
    SELECT
      d.account_executive,
      d.account_executive_id,
      d.deal_id,
      d.deal_name,
      d.amount,
      d.stage_label,
      h.dealstage,
      d.deal_type,
      d.created_date,
      DATE(h.closedate) AS hs_closedate,
      (h.dealstage IS NULL OR h.dealstage NOT IN ('closedwon','closedlost')) AS is_open
    FROM `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
    LEFT JOIN `tractian-bi.pepeuhacker.hubspot_deals_live` h
      ON CAST(h.dealId AS STRING) = d.deal_id
    WHERE d.pipeline_label = 'Sales BR'
      AND d.account_executive IS NOT NULL
    """
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk("pipeline_deals_raw_v6", pipeline), force_refresh=force_refresh)


def forecast_deals_raw(pipeline: str = SALES_BR, force_refresh: bool = False) -> pd.DataFrame:
    """Deals do funil Sales BR nos estágios 1-6 (Scheduled→Procurement) MAIS os
    já ganhos (closedwon = PO Won), com close date, amount e forecast category
    (`hs_manual_forecast_category` do HubSpot raw). O recorte por quarter (close
    date) e a parametrização de líder são feitos em pandas no app — query ampla
    cacheada 1h. Não toca a curated `account_executives_performance`, então roda
    mesmo com a view de comissão fora."""
    sql = """
    SELECT
      d.account_executive,
      d.deal_id,
      d.deal_name,
      d.deal_type,
      d.created_date,
      SAFE_CAST(d.amount AS FLOAT64) AS amount,
      h.dealstage,
      DATE(h.closedate) AS close_date,
      d.po_emission_date,
      h.hs_manual_forecast_category AS forecast_category,
      -- Devices esperados no deal — base do Unit Price (preço médio do sensor).
      -- `mrr_smart_trac`/`contracted_smart_tracs` do HubSpot estão 100% vazias,
      -- então a única quantidade utilizável é a de devices esperados.
      COALESCE(d.expected_devices_smart_trac, 0)
        + COALESCE(d.expected_devices_smart_trac_ex, 0)      AS dev_smart_trac,
      COALESCE(d.expected_devices_energy_trac, 0)
        + COALESCE(d.expected_devices_uni_tracs, 0)
        + COALESCE(d.expected_devices_omni_trac, 0)          AS dev_outros
    FROM `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
    LEFT JOIN `tractian-bi.pepeuhacker.hubspot_deals_live` h
      ON CAST(h.dealId AS STRING) = d.deal_id
    WHERE d.pipeline_label = 'Sales BR'
      AND d.account_executive IS NOT NULL
      AND h.dealstage IN ('appointmentscheduled','contractsent','93878633',
                          '87188634','decisionmakerboughtin','65917881','closedwon')
    """
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk("forecast_deals_raw_v6", pipeline), force_refresh=force_refresh)


def funnel_transitions(t_curr_iso: str, t_prev_iso: str,
                       pipeline: str = SALES_BR, force_refresh: bool = False) -> pd.DataFrame:
    """TODAS as transições de estágio dentro da janela (t_prev, t_curr] — uma
    linha por mudança de stage, com de/para e timestamp.

    Diferente do diff de 2 snapshots (pipeline_wow_snapshot), isto captura o
    caminho INTRA-janela: um deal criado no meio da semana que foi Scheduled →
    Qualifying → Scoping gera 3 linhas (criação + 2 avanços) em vez de sumir
    na comparação de pontas. Base da movimentação do Funil WoW (entrou /
    avançou / lost por etapa). from_stage NULL = criação do deal (1º estágio)."""
    sql = f"""
    WITH ev AS (
      SELECT
        h.dealId,
        s.value      AS to_stage,
        s.timestamp  AS ts,
        LAG(s.value) OVER (PARTITION BY h.dealId ORDER BY s.timestamp) AS from_stage
      FROM `tractian-bi.pepeuhacker.hubspot_deals_history_live` h, UNNEST(h.dealstage) s
    ),
    ae_meta AS (
      SELECT account_executive, leader, region
      FROM `tractian-bi.dbt_analytics_dev.account_executives_performance`
      WHERE region IS NOT NULL AND role = 'AE'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY account_executive ORDER BY quarter DESC) = 1
    )
    SELECT
      d.deal_id,
      d.deal_name,
      d.account_executive,
      ae.leader,
      ae.region,
      COALESCE(SAFE_CAST(d.amount AS FLOAT64), 0) AS amount,
      ev.from_stage,
      ev.to_stage,
      ev.ts
    FROM ev
    JOIN `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
      ON CAST(d.deal_id AS INT64) = ev.dealId
    LEFT JOIN ae_meta ae USING (account_executive)
    WHERE d.pipeline_label = 'Sales BR'
      AND d.account_executive IS NOT NULL
      AND ev.ts >  TIMESTAMP('{t_prev_iso}')
      AND ev.ts <= TIMESTAMP('{t_curr_iso}')
      -- Auto-transição (Scoping → Scoping): o HubSpot regrava o MESMO estágio
      -- no histórico em re-saves; não é movimento e inflava o "entrou"
      -- (Diego Iasbeck: 17+2−3=16 vs 14 reais). from NULL (criação) fica.
      AND (ev.from_stage IS NULL OR ev.from_stage != ev.to_stage)
    """
    key = f"funnel_trans2_{t_prev_iso}_{t_curr_iso}".replace(" ", "_").replace(":", "")
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk(key, pipeline), ttl=86400, force_refresh=force_refresh)


def wow_events(t_curr_iso: str, t_prev_iso: str,
               pipeline: str = SALES_BR, force_refresh: bool = False) -> pd.DataFrame:
    """TODOS os eventos que mexem no pipe de um deal dentro da janela
    (t_prev, t_curr] — uma linha por evento, de dois tipos:

      kind='stage'   mudança de estágio  → stage_from/stage_to, amount_at
      kind='amount'  mudança de valor    → amt_from/amt_to,     stage_at

    Existe pra substituir o diff de pontas do `pipeline_wow_snapshot` no WoW de
    Early/Late. Comparar só as 2 pontas perde o caminho intra-semana e, no pior
    caso, PERDE O DEAL: criado na quarta e ganho na sexta tem stage_prev=NULL e
    stage_curr='closedwon', não casa com nenhuma máscara de `_wow_categorize` e
    sumia como 'irrelevant'/delta 0. Mesmo remédio já aplicado no Funil WoW
    (`funnel_transitions`), aqui estendido pro valor, porque esta tela acompanha
    R$ além de estágio.

    Cada evento carrega o estado AS-OF do outro eixo, e é isso que faz a ponte
    fechar: um 'stage' traz `amount_at` (valor vigente no instante do
    movimento, não o amount atual do deal) e um 'amount' traz `stage_at` (onde o
    deal estava quando o valor mudou) — sem isso não há como saber se o
    cresceu/encolheu aconteceu DENTRO do foco.

    Os LAG rodam sobre o histórico INTEIRO e o corte de janela vem depois, senão
    o 1º evento da semana perderia o `from` (viria NULL como se fosse criação).
    Descartadas as regravações do mesmo valor: auto-transição de estágio
    (Scoping → Scoping em re-save, que inflava o "entrou" no Funil) e amount
    reescrito igual."""
    sql = f"""
    WITH stage_ev AS (
      SELECT
        h.dealId,
        'stage'      AS kind,
        s.timestamp  AS ts,
        LAG(s.value) OVER (PARTITION BY h.dealId ORDER BY s.timestamp) AS stage_from,
        s.value      AS stage_to,
        CAST(NULL AS STRING) AS stage_at,
        CAST(NULL AS FLOAT64) AS amt_from,
        CAST(NULL AS FLOAT64) AS amt_to,
        -- ESTRITAMENTE `<`, não `<=`: se o deal muda de estágio E de valor no
        -- MESMO timestamp (comum — um save mexe nos dois), o `<=` faria este
        -- evento já ver o valor novo enquanto o evento de amount, cujo stage_at
        -- usa `<=`, já vê o estágio novo. Os dois contariam a mesma variação, e
        -- o erro sai simétrico entre os focos (Early −X / Late +X; foi
        -- exatamente R$ 51.691 na janela 26/07→02/08).
        -- Convenção resultante no empate: aplica-se primeiro o ESTÁGIO (com o
        -- valor antigo) e depois o VALOR (já no estágio novo). Assim
        -- v_antes/v_depois encadeiam sem buraco e a soma telescopa.
        (SELECT SAFE_CAST(a.value AS FLOAT64) FROM UNNEST(h.amount) a
           WHERE a.timestamp < s.timestamp
           ORDER BY a.timestamp DESC LIMIT 1) AS amount_at
      FROM `tractian-bi.pepeuhacker.hubspot_deals_history_live` h, UNNEST(h.dealstage) s
    ),
    amount_ev AS (
      SELECT
        h.dealId,
        'amount'     AS kind,
        a.timestamp  AS ts,
        CAST(NULL AS STRING) AS stage_from,
        CAST(NULL AS STRING) AS stage_to,
        (SELECT s.value FROM UNNEST(h.dealstage) s
           WHERE s.timestamp <= a.timestamp
           ORDER BY s.timestamp DESC LIMIT 1) AS stage_at,
        LAG(SAFE_CAST(a.value AS FLOAT64))
          OVER (PARTITION BY h.dealId ORDER BY a.timestamp) AS amt_from,
        SAFE_CAST(a.value AS FLOAT64) AS amt_to,
        SAFE_CAST(a.value AS FLOAT64) AS amount_at
      FROM `tractian-bi.pepeuhacker.hubspot_deals_history_live` h, UNNEST(h.amount) a
    ),
    ev AS (
      SELECT dealId, kind, ts, stage_from, stage_to, stage_at,
             amt_from, amt_to, amount_at
      FROM stage_ev
      WHERE stage_from IS NULL OR stage_from != stage_to
      UNION ALL
      SELECT dealId, kind, ts, stage_from, stage_to, stage_at,
             amt_from, amt_to, amount_at
      FROM amount_ev
      -- amt_from NULL = 1ª vez que o deal recebe valor. NÃO descartar: sem esse
      -- evento o deal que nasce sem amount e é preenchido DEPOIS de entrar no
      -- estágio some da conta (a entrada valeu 0 e o preenchimento não conta),
      -- e a ponte não fecha. Valor não preenchido = 0, igual ao COALESCE do
      -- pipeline_wow_snapshot, então NULL → 0 é consistente com as pontas.
      WHERE amt_from IS NULL OR amt_from != amt_to
    ),
    ae_meta AS (
      SELECT account_executive, leader, region
      FROM `tractian-bi.dbt_analytics_dev.account_executives_performance`
      WHERE region IS NOT NULL AND role = 'AE'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY account_executive ORDER BY quarter DESC) = 1
    )
    SELECT
      d.deal_id,
      d.deal_name,
      d.account_executive,
      ae.leader,
      ae.region,
      -- Deal type (Aquisition / Up & cross sell). Carregado nas 3 fontes do WoW,
      -- e não por join no app, porque nível e movimento são frames SEPARADOS:
      -- só levando o tipo em cada um o filtro recorta os dois igualzinho.
      d.deal_type,
      ev.kind,
      ev.ts,
      ev.stage_from,
      ev.stage_to,
      ev.stage_at,
      COALESCE(ev.amt_from, 0)   AS amt_from,
      COALESCE(ev.amt_to, 0)     AS amt_to,
      COALESCE(ev.amount_at, 0)  AS amount_at
    FROM ev
    JOIN `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
      ON CAST(d.deal_id AS INT64) = ev.dealId
    LEFT JOIN ae_meta ae USING (account_executive)
    WHERE d.pipeline_label = 'Sales BR'
      AND d.account_executive IS NOT NULL
      AND ev.ts >  TIMESTAMP('{t_prev_iso}')
      AND ev.ts <= TIMESTAMP('{t_curr_iso}')
    """
    key = f"wow_events_v2_{t_prev_iso}_{t_curr_iso}".replace(" ", "_").replace(":", "")
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk(key, pipeline), ttl=86400, force_refresh=force_refresh)


def unit_price_events(force_refresh: bool = False) -> pd.DataFrame:
    """Eventos de venda VALIDADA de Smart Trac — base do Unit Price.

    Fonte = `monthly_recurring_revenue` (ledger de eventos de MRR por produto,
    alimentado pelo REGISTRO DE VENDA do ops/finance), não o HubSpot: os campos
    `expected_devices_*` do deal foram abandonados (Q2/26 só 27/210 ganhos
    preenchidos, Q3/26 zero) e subcontavam. Aqui é a venda validada linha a
    linha: MRR do sensor e quantidade de sensores (product_adjusted='Smart
    Trac' exclui a linha de Gateway do mesmo produto).

    'First PO' = venda nova; 'Upsell'/'Cross sell' = expansão. Churn/Downsell/
    Price adjustment ficam fora (não são venda). `date` ≈ po_emission_date
    (mesmo dia em 705/713 desde Q3/25), então o corte trimestral é comparável
    às demais séries do Sales Overtime. Sub-linhas do mesmo deal/dia/tipo são
    somadas (1 linha por evento de venda)."""
    sql = """
    SELECT
      CAST(m.deal_id AS STRING)   AS deal_id,
      ANY_VALUE(m.deal_name)      AS deal_name,
      ANY_VALUE(m.account_executive) AS account_executive,
      DATE(m.date)                AS event_date,
      m.deal_sale_type,
      SUM(m.amount_in_brl)        AS amount_brl,
      SUM(m.devices)              AS devices
    FROM `tractian-bi.dbt_analytics_dev.monthly_recurring_revenue` m
    WHERE m.region IN ('BR','Brazil')
      AND m.product_adjusted = 'Smart Trac'
      AND m.deal_sale_type IN ('First PO','Upsell','Cross sell')
      AND m.devices > 0
    GROUP BY deal_id, event_date, deal_sale_type
    """
    return query(sql, cache_key="unit_price_events_v1", force_refresh=force_refresh)


def pipeline_wow_snapshot(t_curr_iso: str, t_prev_iso: str,
                          pipeline: str = SALES_BR, force_refresh: bool = False) -> pd.DataFrame:
    """Reconstrói o estado de cada deal do funil Sales BR em 2 timestamps
    (t_curr e t_prev) usando hubspot_deals_history (array de {value, timestamp}).
    Para cada deal devolve amount/stage_raw em cada um dos snapshots, mais
    metadata do AE (account_executive, leader, region).

    `t_curr_iso` e `t_prev_iso` em formato ISO com timezone (ex.: '2026-05-03 15:00:00 UTC').
    SAFE_CAST cobre amounts vazios. Universo restrito a pipeline_label='Sales BR'
    e linhas que tenham stage em pelo menos um dos 2 snapshots."""
    sql = f"""
    WITH hist AS (
      SELECT
        h.dealId,
        (SELECT a.value FROM UNNEST(h.amount) a
           WHERE a.timestamp <= TIMESTAMP('{t_curr_iso}')
           ORDER BY a.timestamp DESC LIMIT 1) AS amt_curr,
        (SELECT a.value FROM UNNEST(h.amount) a
           WHERE a.timestamp <= TIMESTAMP('{t_prev_iso}')
           ORDER BY a.timestamp DESC LIMIT 1) AS amt_prev,
        (SELECT s.value FROM UNNEST(h.dealstage) s
           WHERE s.timestamp <= TIMESTAMP('{t_curr_iso}')
           ORDER BY s.timestamp DESC LIMIT 1) AS stage_curr,
        (SELECT s.value FROM UNNEST(h.dealstage) s
           WHERE s.timestamp <= TIMESTAMP('{t_prev_iso}')
           ORDER BY s.timestamp DESC LIMIT 1) AS stage_prev
      FROM `tractian-bi.pepeuhacker.hubspot_deals_history_live` h
    ),
    ae_meta AS (
      SELECT account_executive, leader, region
      FROM `tractian-bi.dbt_analytics_dev.account_executives_performance`
      WHERE region IS NOT NULL AND role = 'AE'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY account_executive ORDER BY quarter DESC) = 1
    )
    SELECT
      d.deal_id,
      d.deal_name,
      d.account_executive,
      ae.leader,
      ae.region,
      -- Deal type (Aquisition / Up & cross sell). Carregado nas 3 fontes do WoW,
      -- e não por join no app, porque nível e movimento são frames SEPARADOS:
      -- só levando o tipo em cada um o filtro recorta os dois igualzinho.
      d.deal_type,
      COALESCE(SAFE_CAST(hist.amt_curr AS FLOAT64), 0) AS amount_curr,
      COALESCE(SAFE_CAST(hist.amt_prev AS FLOAT64), 0) AS amount_prev,
      hist.stage_curr,
      hist.stage_prev
    FROM hist
    JOIN `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
      ON CAST(d.deal_id AS INT64) = hist.dealId
    LEFT JOIN ae_meta ae USING (account_executive)
    WHERE d.pipeline_label = 'Sales BR'
      AND d.account_executive IS NOT NULL
      AND (hist.stage_curr IS NOT NULL OR hist.stage_prev IS NOT NULL)
    """
    # Cache key inclui ambos timestamps — invalida quando muda a janela
    # (v2 = +deal_name, v3 = +deal_type)
    key = f"pipe_wow_v3_{t_prev_iso}_{t_curr_iso}".replace(" ", "_").replace(":", "")
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk(key, pipeline), ttl=86400, force_refresh=force_refresh)


def pipeline_level_series(sundays_iso: list[str], stage_ids: list[str],
                          pipeline: str = SALES_BR, force_refresh: bool = False) -> pd.DataFrame:
    """Estado (estágio + amount) de cada deal do funil Sales BR em VÁRIOS
    timestamps de uma vez — uma linha por (deal, timestamp). `week_idx` é a
    posição do timestamp na lista informada (0 = o primeiro).

    Existe pro Histórico PoP, que precisa do NÍVEL do pipe em N domingos. A
    alternativa seria chamar `pipeline_wow_snapshot` N vezes, ou seja N
    varreduras do histórico; o CROSS JOIN com a lista de timestamps resolve
    numa varredura só.

    >>> POR QUE O NÍVEL NÃO SAI DA SOMA TELESCÓPICA DOS EVENTOS

    Seria de graça: o Histórico já carrega `wow_events` pro movimento, e a soma
    dos deltas telescopa em v(t_final) − v(t_inicial) por construção (é o que a
    tela PoP Early/Late usa na ponte do rodapé). Medido domingo a domingo nas 12
    semanas até 16/08/2026, porém, ela NÃO reproduz o snapshot real:

        Early  R$ 0,00 até 02/08, R$ 18,27 de erro daí em diante
        Late   erro em degraus até R$ 65,9 mil (0,9% do pipe), em 3 deals

    O erro é DEGRAU, não ruído — aparece num deal específico e fica. Duas causas,
    as duas de dado real:

      1) amount APAGADO no HubSpot (valor vazio). `SAFE_CAST` vira NULL, e o
         filtro `amt_from IS NULL OR amt_from != amt_to` do `wow_events` derruba
         a linha (NULL != x é NULL, não TRUE) — mas o snapshot lê o mesmo evento
         como NULL → COALESCE 0. O evento some do movimento e aparece no nível.
         Deals 62704556341 (R$ 18,27) e 59419035019 (R$ 3 mil).

      2) deal que ENTRA e SAI como won dentro da janela, com o 1º evento de valor
         sem antecessor: o `novo` entra com `amount_at` cheio e o `won` sai com
         outro valor, deixando resíduo. Deals 59728264720 (R$ 52,9 mil) e
         60251592070 (R$ 10 mil).

    Então movimento (eventos) e nível (snapshot) são fontes SEPARADAS de
    propósito — a mesma decisão que a tela PoP Early/Late já havia tomado por
    outro motivo. Se um dia as duas causas forem corrigidas na origem, dá pra
    trocar esta query pela soma acumulada e economizar a varredura; até lá,
    derivar o nível dos eventos poria 0,9% de erro silencioso na linha.

    `stage_ids` é a allowlist de estágios que contam como pipe: vem do app, que
    é dono da classificação early/late, pra não existir uma 2ª cópia da lista
    aqui e as duas divergirem na próxima mudança de funil."""
    _ts_arr = ", ".join(f"TIMESTAMP('{t}')" for t in sundays_iso)
    _stages = ", ".join(f"'{s}'" for s in stage_ids)
    sql = f"""
    WITH ts_list AS (
      SELECT ts, off AS week_idx
      FROM UNNEST([{_ts_arr}]) AS ts WITH OFFSET AS off
    ),
    snaps AS (
      SELECT
        h.dealId,
        t.week_idx,
        (SELECT a.value FROM UNNEST(h.amount) a
           WHERE a.timestamp <= t.ts
           ORDER BY a.timestamp DESC LIMIT 1) AS amt,
        (SELECT s.value FROM UNNEST(h.dealstage) s
           WHERE s.timestamp <= t.ts
           ORDER BY s.timestamp DESC LIMIT 1) AS stage
      FROM `tractian-bi.pepeuhacker.hubspot_deals_history_live` h
      CROSS JOIN ts_list t
    ),
    ae_meta AS (
      SELECT account_executive, leader, region
      FROM `tractian-bi.dbt_analytics_dev.account_executives_performance`
      WHERE region IS NOT NULL AND role = 'AE'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY account_executive ORDER BY quarter DESC) = 1
    )
    SELECT
      d.deal_id,
      d.deal_name,
      d.account_executive,
      ae.leader,
      ae.region,
      -- Deal type (Aquisition / Up & cross sell). Carregado nas 3 fontes do WoW,
      -- e não por join no app, porque nível e movimento são frames SEPARADOS:
      -- só levando o tipo em cada um o filtro recorta os dois igualzinho.
      d.deal_type,
      snaps.week_idx,
      snaps.stage,
      COALESCE(SAFE_CAST(snaps.amt AS FLOAT64), 0) AS amount
    FROM snaps
    JOIN `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
      ON CAST(d.deal_id AS INT64) = snaps.dealId
    LEFT JOIN ae_meta ae USING (account_executive)
    WHERE d.pipeline_label = 'Sales BR'
      AND d.account_executive IS NOT NULL
      AND snaps.stage IN ({_stages})
    """
    # A chave tem que distinguir a JANELA INTEIRA, não só as pontas: 8 e 12
    # semanas terminam no mesmo domingo e colidiriam numa chave só de extremos.
    key = ("pipe_level_v2_" + hashlib.md5(
        ("|".join(sundays_iso) + "#" + "|".join(stage_ids)).encode()).hexdigest()[:16])
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk(key, pipeline), ttl=86400, force_refresh=force_refresh)


# ===========================================================================
# Estado REAL de cada domingo — os snapshots do próprio BI.
#
# `materialized_data_source` guarda uma CÓPIA DIÁRIA da curated
# `int_adding_names_to_deals` (161 tabelas de 23/03/2026 a 30/08/2026, sem furo,
# gravadas no próprio dia às ~20:16 UTC). Cada cópia tem, as-of daquele dia:
# `deal_stage_id`, `amount`, `close_date`, `deal_type`, `po_emission_date` e
# `hs_manual_forecast_category`. (As outras duas famílias de snapshot do dataset
# têm rótulo defasado em 6 dias e ficam fora — ver `forecast_asof_snapshots`.)
#
# >>> POR QUE ISSO SUBSTITUI A RECONSTRUÇÃO ANTERIOR (e não é só mais uma fonte)
#
# A série do Histórico do Forecast era montada de duas fontes indiretas, e as
# DUAS erravam:
#
#   1) `close_date_history_df` (WIDE, 20 pares) está congelada desde
#      30/07/2026 → para todo domingo posterior ela devolve a data de 30/07, e
#      quem escorregou depois continuava contando dentro do quarter. Medido no
#      Q3/2026, domingo 30/08: R$ 9,24 mi contra R$ 6,04 mi reais (+53%).
#
#   2) O fallback `cd_first` (o valor mais antigo conhecido, usado nos domingos
#      anteriores à 1ª mudança registrada) inventava close date para deals que
#      naquela data AINDA NÃO TINHAM close date nenhum. Medido em 05/07/2026:
#      547 deals e R$ 4,23 mi de nível vindos daí — um terço da série — quase
#      todos com `close_date` NULO na cópia real do dia.
#
# Some-se a isso que `hubspot_deals_history` versiona só 7 campos (nenhum é a
# forecast category nem o close date), e o resultado é que a tela mostrava
# forecast BRUTO inflado. Com os snapshots do BI as duas séries ponderadas —
# Modelo Analytics (estágio × segmento × tipo) e Modelo dos diretores
# (forecast category 100/75/50/25%) — passam a existir para o quarter inteiro,
# com o valor que valia em cada domingo.
# ===========================================================================

FC_ASOF_DATASET = "tractian-bi.materialized_data_source"
FC_ASOF_TABLE = "int_adding_names_to_deals"


def forecast_asof_snapshots(force_refresh: bool = False) -> pd.DataFrame:
    """Que dias têm cópia as-of confiável, e sob qual sufixo de tabela.

    SÓ a família DIÁRIA `int_adding_names_to_deals<YYYYMMDD>` (23/03/2026 em
    diante, 161 tabelas sem furo). Ela é gravada no PRÓPRIO dia do rótulo, às
    ~20:16 UTC — lag 0 em 161/161 medidas.

    Duas outras famílias do mesmo dataset ficam FORA porque o rótulo delas mente:
      · `int_adding_names_to_deals_<YYYYMMDD>` (com underscore, desde
        15/02/2026): `creation_time` mostra +6 dias em 6/6 casos, e a prova está
        no dado — `_20260322` tem 31.370 linhas e Σamount R$ 111.439.987,68,
        praticamente idêntica à DIÁRIA de `20260328` (31.371 · R$ 111.460.387,68),
        e não à de 20260323 (31.069 · R$ 109.202.501,83). Usá-la deslocaria a
        série uma semana à esquerda;
      · `deals_weekly_materialization_<data>` (desde nov/2024): mesmo defeito,
        `snapshot_time` no sábado seguinte ao rótulo em 9 dos 10 domingos do
        Q3/2026 medidos.

    O preço de excluir as duas é não ter série antes de 23/03/2026 — e a tela diz
    isso, em vez de desenhar uma curva deslocada."""
    sql = f"""
    SELECT table_name
    FROM `{FC_ASOF_DATASET}.INFORMATION_SCHEMA.TABLES`
    WHERE REGEXP_CONTAINS(table_name, r'^{FC_ASOF_TABLE}[0-9]{{8}}$')
    """
    df = query(sql, cache_key="fc_asof_snapshots_v2", ttl=3600,
               force_refresh=force_refresh)
    if not len(df):
        return pd.DataFrame(columns=["dia", "sufixo"])
    n = len(FC_ASOF_TABLE)
    rows = [{"dia": t[n:], "sufixo": t[n:]} for t in df["table_name"]]
    return pd.DataFrame(rows, columns=["dia", "sufixo"]).sort_values("dia")


def forecast_asof_series(sufixos: list[str], open_stage_ids: list[str],
                         won_stage_id: str = "closedwon",
                         lost_stage_id: str | None = None,
                         lost_from: str | None = None, lost_to: str | None = None,
                         pipeline: str = SALES_BR,
                         force_refresh: bool = False) -> pd.DataFrame:
    """Uma linha por (deal, dia) com o estado REAL do deal naquele dia, lido das
    cópias diárias do BI. `sufixos` vem de `forecast_asof_snapshots`.

    Devolve o dia como `dia` (YYYYMMDD, string) — o app converte pra domingo e
    faz o recorte de quarter, que é dele. `open_stage_ids` vem do app pelo mesmo
    motivo de sempre: a definição de "estágio aberto 1-6" tem um
    dono só. O `won_stage_id` entra junto porque a série precisa enxergar o deal
    que saiu do forecast POR TER SIDO GANHO — sem ele, won e escorregada viram o
    mesmo movimento.

    >>> LOST É OPCIONAL, E COM JANELA DE DATA

    Por padrão o fechado-perdido NÃO vem: pra série do Histórico do Forecast a
    ausência dele no conjunto aberto já é a saída, e trazer o perdido inteiro
    quintuplica o volume (67 mil linhas em 4 dias contra 13 mil com o filtro) —
    são ~15,5 mil deals perdidos POR SNAPSHOT, o histórico de perda desde
    sempre. O WoW do Forecast precisa da faixa Lost como bucket próprio, e pede
    lost com `lost_from`/`lost_to`: só o perdido cujo close date cai na janela
    (o HubSpot escreve o close date no dia em que o deal é marcado perdido, então
    "close date no trimestre" = "perdido no trimestre"). Com o recorte são ~1,5
    mil linhas por snapshot em vez de 15,5 mil.

    A janela do lost entra na chave de cache: sem isso o mesmo par de domingos
    devolveria o parquet de quem pediu sem lost."""
    if not sufixos:
        return pd.DataFrame(columns=["dia", "deal_id", "deal_name", "account_executive",
                                     "deal_type", "stage", "amount", "close_date",
                                     "forecast_category", "po_emission_date"])
    _in = ", ".join(f"'{x}'" for x in sufixos)
    _stages = ", ".join(f"'{x}'" for x in list(open_stage_ids) + [won_stage_id])
    # `lost_stage_id` sozinho não basta: sem janela ele traria a perda histórica
    # inteira, que é justamente o que o filtro existe pra evitar.
    _com_lost = bool(lost_stage_id and lost_from and lost_to)
    _lost = (f"""
        OR (deal_stage_id = '{lost_stage_id}'
            AND close_date BETWEEN '{lost_from}' AND '{lost_to}')"""
             if _com_lost else "")
    sql = f"""
    SELECT
      REPLACE(_TABLE_SUFFIX, '_', '')        AS dia,
      CAST(deal_id AS STRING)                AS deal_id,
      deal_name,
      account_executive,
      deal_type,
      deal_stage_id                          AS stage,
      SAFE_CAST(amount AS FLOAT64)           AS amount,
      close_date,
      hs_manual_forecast_category            AS forecast_category,
      po_emission_date
    FROM `{FC_ASOF_DATASET}.{FC_ASOF_TABLE}*`
    WHERE _TABLE_SUFFIX IN ({_in})
      AND pipeline_label = 'Sales BR'
      AND account_executive IS NOT NULL
      AND ( deal_stage_id IN ({_stages}){_lost} )
    """
    # A janela inteira entra na chave (duas janelas podem terminar no mesmo
    # domingo). O trecho do lost é acrescentado SÓ quando pedido, pra todo
    # parquet já gravado sem lost continuar valendo.
    _k = "|".join(sufixos) + "#" + "|".join(open_stage_ids)
    if _com_lost:
        _k += f"#lost:{lost_stage_id}:{lost_from}:{lost_to}"
    key = "fc_asof_v1_" + hashlib.md5(_k.encode()).hexdigest()[:16]
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk(key, pipeline), ttl=86400,
                 force_refresh=force_refresh)


def pipeline_asof_changes(sufixos: list[str], stage_ids: list[str],
                          pipeline: str = SALES_BR,
                          force_refresh: bool = False) -> pd.DataFrame:
    """Estado DIÁRIO (estágio, amount, close date) dos deals que passaram por
    `stage_ids` em algum dos dias de `sufixos` — mas só as linhas em que o
    estado MUDOU em relação à cópia anterior da lista. Base do PoP Histórico
    por quarter, que precisa do close date as-of de cada dia (e só as cópias do
    BI o têm: `hubspot_deals_history` não versiona close date).

    >>> POR QUE SÓ AS MUDANÇAS

    Em 26 semanas são 183 cópias × ~1,5 mil deals = 281 mil linhas, quase todas
    repetindo a véspera. Emitir só a 1ª linha de cada deal e as viradas de
    estado dá alguns milhares; o app reconstrói a grade com ffill. A AUSÊNCIA
    também é estado (linha com `stage` NULO): é o deal que saiu da cópia —
    apagado, trocou de pipeline ou perdeu o AE — e a tela precisa disso pra
    não confundir "sumiu" com "continua igual".

    `sufixos` já vem só com as cópias SAUDÁVEIS (o app tira 01/10/2026 e afins):
    a ordem da lista é a ordem do tempo, e uma cópia sem AE no meio dela faria
    o funil inteiro sair e voltar num dia.

    Dono, nome e tipo do deal são os da cópia MAIS RECENTE em que ele aparece —
    a mesma convenção do PoP Histórico, que atribui o histórico ao dono atual.

    Close date antes de 2000 vira NULO: desde 04/10/2026 a cópia grava close
    date vazio como 1969-12-31/1970-01-01 (ver `hubspot_deals_live.sql`)."""
    if not sufixos:
        return pd.DataFrame(columns=["dia", "deal_id", "deal_name", "account_executive",
                                     "deal_type", "stage", "amount", "close_date"])
    _in = ", ".join(f"'{x}'" for x in sufixos)
    _stages = ", ".join(f"'{x}'" for x in stage_ids)
    sql = f"""
    WITH dias AS (
      SELECT dia, ord FROM UNNEST([{_in}]) AS dia WITH OFFSET ord
    ),
    base AS (
      SELECT
        _TABLE_SUFFIX                                AS dia,
        CAST(deal_id AS STRING)                      AS deal_id,
        deal_name,
        account_executive,
        deal_type,
        deal_stage_id                                AS stage,
        COALESCE(SAFE_CAST(amount AS FLOAT64), 0)    AS amount,
        IF(close_date < DATE '2000-01-01', NULL, close_date) AS close_date
      FROM `{FC_ASOF_DATASET}.{FC_ASOF_TABLE}*`
      WHERE _TABLE_SUFFIX IN ({_in})
        AND pipeline_label = 'Sales BR'
        AND account_executive IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (PARTITION BY _TABLE_SUFFIX, deal_id ORDER BY amount DESC) = 1
    ),
    alvo AS (
      SELECT DISTINCT deal_id FROM base WHERE stage IN ({_stages})
    ),
    meta AS (
      SELECT deal_id,
             ARRAY_AGG(STRUCT(deal_name, account_executive, deal_type)
                       ORDER BY dia DESC LIMIT 1)[OFFSET(0)] AS m
      FROM base JOIN alvo USING (deal_id)
      GROUP BY deal_id
    ),
    grade AS (
      SELECT a.deal_id, d.dia, d.ord, b.stage, b.amount, b.close_date
      FROM alvo a
      CROSS JOIN dias d
      LEFT JOIN base b ON b.deal_id = a.deal_id AND b.dia = d.dia
    ),
    mud AS (
      SELECT *,
        TO_JSON_STRING(STRUCT(stage, amount, close_date)) AS est,
        LAG(TO_JSON_STRING(STRUCT(stage, amount, close_date)))
          OVER (PARTITION BY deal_id ORDER BY ord) AS est_ant
      FROM grade
    )
    SELECT
      mud.dia, mud.deal_id,
      meta.m.deal_name, meta.m.account_executive, meta.m.deal_type,
      mud.stage, mud.amount, mud.close_date
    FROM mud JOIN meta USING (deal_id)
    WHERE mud.est_ant IS NULL OR mud.est_ant != mud.est
    """
    _k = "|".join(sufixos) + "#" + "|".join(stage_ids)
    key = "pipe_asof_chg_v1_" + hashlib.md5(_k.encode()).hexdigest()[:16]
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk(key, pipeline), ttl=86400,
                 force_refresh=force_refresh)


def won_asof(sufixos: list[str], pipeline: str = SALES_BR,
             force_refresh: bool = False) -> pd.DataFrame:
    """Realizado do trimestre COMO ESTAVA em cada cópia diária do BI: deals
    `closedwon` com PO entre o início do trimestre da cópia e o dia dela. Base
    da quota remanescente da cobertura histórica do PoP Histórico Q.

    Por que a cópia e não o estado de hoje (`_get_forecast_deals`): o pipe da
    mesma tela sai das cópias, e só a mesma base garante que um deal esteja em
    UM lado da conta por domingo. Com o estado de hoje, 26 deals (R$ 664 mil)
    ganhos em 01–02/10/2026 com PO datado de 28–30/09 apareciam no realizado de
    30/09 E no pipe Late desse dia.

    Validado e hold vêm juntos (os dois abatem a quota, e o hold é won com PO);
    a separação entre eles não existe nas cópias. O app aplica a extensão de
    fechamento (`_rev_date`) e a atribuição de líder."""
    if not sufixos:
        return pd.DataFrame(columns=["dia", "deal_id", "deal_name", "account_executive",
                                     "deal_type", "amount", "po_emission_date"])
    _in = ", ".join(f"'{x}'" for x in sufixos)
    sql = f"""
    SELECT
      _TABLE_SUFFIX                              AS dia,
      CAST(deal_id AS STRING)                    AS deal_id,
      deal_name,
      account_executive,
      deal_type,
      COALESCE(SAFE_CAST(amount AS FLOAT64), 0)  AS amount,
      po_emission_date
    FROM `{FC_ASOF_DATASET}.{FC_ASOF_TABLE}*`
    WHERE _TABLE_SUFFIX IN ({_in})
      AND pipeline_label = 'Sales BR'
      AND account_executive IS NOT NULL
      AND deal_stage_id = 'closedwon'
      AND po_emission_date BETWEEN DATE_TRUNC(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX), QUARTER)
                               AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _TABLE_SUFFIX, deal_id ORDER BY amount DESC) = 1
    """
    key = "won_asof_v1_" + hashlib.md5("|".join(sufixos).encode()).hexdigest()[:16]
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk(key, pipeline), ttl=86400,
                 force_refresh=force_refresh)


FC_SNAPSHOT_TABLE = "tractian-bi.pepeuhacker.forecast_snapshots"


def forecast_snapshot_write(sunday_iso: str, open_stage_ids: list[str],
                            won_stage_id: str = "closedwon") -> int:
    """Grava o snapshot semanal do forecast. Devolve nº de linhas inseridas
    (0 = o domingo já estava gravado, nada a fazer).

    >>> POR QUE ESTA TABELA EXISTE

    `hs_manual_forecast_category` — o campo que dá o PESO do forecast (Commit
    100 · Best Case 75 · Probable 50 · Funnel 25) — não é versionada em lugar
    nenhum do BigQuery: `hubspot_deals_history` versiona 7 campos e nenhum é ela,
    e `mx_pipeline_snapshots` é Sales MX. Resultado: a série ponderada do
    Histórico do Forecast NÃO é reconstruível pro passado, e a tela mostra o
    forecast bruto. Gravando aqui toda semana, ela passa a existir daqui pra
    frente — sem inventar nada sobre o que já passou.

    Grava também `close_date`, que hoje só tem histórico em
    `dbt_analytics_dev.close_date_history_df` — uma tabela ESTÁTICA, congelada
    em 30/07/2026. Ou seja: esta tabela conserta
    duas lacunas, não uma.

    IDEMPOTENTE por domingo: o INSERT é guardado por um NOT EXISTS na própria
    tabela, então rodar de novo no mesmo domingo não duplica. Escrever o guard
    dentro do próprio INSERT (e não como um SELECT antes) evita a corrida entre
    duas instâncias do app acordando juntas.

    A captura acontece na 1ª oportunidade DEPOIS do domingo, não às 12h em
    ponto — o app chama isto do laço de reaquecimento. Por isso `captured_at`
    guarda o instante real: a defasagem fica medível em vez de suposta."""
    _stages = ", ".join(f"'{x}'" for x in list(open_stage_ids) + [won_stage_id])
    sql = f"""
    INSERT INTO `{FC_SNAPSHOT_TABLE}`
      (snapshot_date, captured_at, deal_id, deal_name, account_executive,
       stage, amount, close_date, forecast_category)
    SELECT
      DATE('{sunday_iso}')            AS snapshot_date,
      CURRENT_TIMESTAMP()             AS captured_at,
      d.deal_id,
      d.deal_name,
      d.account_executive,
      h.dealstage                     AS stage,
      SAFE_CAST(d.amount AS FLOAT64)  AS amount,
      DATE(h.closedate)               AS close_date,
      h.hs_manual_forecast_category   AS forecast_category
    FROM `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
    LEFT JOIN `tractian-bi.pepeuhacker.hubspot_deals_live` h
      ON CAST(h.dealId AS STRING) = d.deal_id
    WHERE d.pipeline_label = 'Sales BR'
      AND d.account_executive IS NOT NULL
      AND h.dealstage IN ({_stages})
      AND NOT EXISTS (
        SELECT 1 FROM `{FC_SNAPSHOT_TABLE}` s
        WHERE s.snapshot_date = DATE('{sunday_iso}')
      )
    """
    job = client().query(sql)
    job.result()
    return int(job.num_dml_affected_rows or 0)


# ===========================================================================
# FOTO DA MEIA-NOITE — base do DoD do Forecast
# ===========================================================================
# A cópia diária do BI (`materialized_data_source`) é gravada às ~20:16 UTC, ou
# seja 17h16 em Brasília: ela NÃO é a meia-noite, e comparar duas delas mede
# "17h de ontem → 17h de hoje", não o dia. Pra comparar a virada de um dia com a
# do anterior, o app tira a própria foto logo depois das 00:00 BRT.
#
# Não dá pra reconstruir a meia-noite do passado: `hubspot_deals_history`
# versiona estágio e amount, mas nem close date nem forecast category. Por isso a
# série própria começa em 24/09/2026, e antes disso o DoD usa a cópia do BI da
# véspera como APROXIMAÇÃO — e diz isso na tela (ver `_fcd_sides` no app).
# ===========================================================================

FC_DAILY_TABLE = "tractian-bi.pepeuhacker.forecast_snapshots_daily"


def forecast_asof_ae_counts(sufixos: list[str], pipeline: str = SALES_BR,
                            force_refresh: bool = False) -> pd.DataFrame:
    """Quantos deals do Sales BR têm AE em cada cópia diária do BI — o teste de
    sanidade da cópia antes de ela virar ponta aproximada do DoD.

    Existe por causa de 01/10/2026: no dia em que o schema da nekt mudou de dono
    (ver `hubspot_deals_live.sql`), a cópia do BI saiu com `account_executive`
    NULO em 100% das linhas, e a de 02/10 com 26% a menos. Como ponta, a de
    01/10 fazia o pipe inteiro "sair do funil" num dia. Custa 2 colunas de
    ~16 tabelas."""
    if not sufixos:
        return pd.DataFrame(columns=["sufixo", "n_ae"])
    _in = ", ".join(f"'{x}'" for x in sufixos)
    sql = f"""
    SELECT _TABLE_SUFFIX AS sufixo, COUNTIF(account_executive IS NOT NULL) AS n_ae
    FROM `{FC_ASOF_DATASET}.{FC_ASOF_TABLE}*`
    WHERE _TABLE_SUFFIX IN ({_in}) AND pipeline_label = 'Sales BR'
    GROUP BY 1
    """
    key = "fc_asof_ae_counts_v1_" + hashlib.md5("|".join(sorted(sufixos)).encode()).hexdigest()[:16]
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk(key, pipeline), ttl=21600,
                 force_refresh=force_refresh)


def forecast_daily_write(day_iso: str, open_stage_ids: list[str],
                         won_stage_id: str = "closedwon",
                         lost_stage_id: str = "closedlost",
                         pipeline: str = SALES_BR) -> int:
    """Grava a foto da meia-noite de `day_iso`. Devolve nº de linhas (0 = o dia
    já estava gravado).

    Mesmo recorte de `forecast_asof_series`, pra as duas fontes serem
    intercambiáveis no DoD: Sales BR, com AE, nos 6 estágios abertos + Won — mais
    o Lost dos últimos ~200 dias (cobre o trimestre corrente com folga; o
    perdido histórico inteiro são ~15 mil linhas por dia que ninguém lê).

    Estado (estágio, close date, forecast category) vem da view viva, como no
    snapshot semanal; amount, AE, deal type e data do PO vêm da curated.

    IDEMPOTENTE pelo mesmo NOT EXISTS do semanal. Duas instâncias acordando no
    mesmo segundo ainda podem gravar em dobro — por isso a LEITURA fica com uma
    captura só por dia (a mais cedo)."""
    _stages = ", ".join(f"'{x}'" for x in list(open_stage_ids) + [won_stage_id])
    sql = f"""
    INSERT INTO `{FC_DAILY_TABLE}`
      (snapshot_date, captured_at, source_at, deal_id, deal_name,
       account_executive, deal_type, stage, amount, close_date,
       forecast_category, po_emission_date, pipeline)
    WITH fonte AS (
      SELECT MAX(hs_lastmodifieddate) AS source_at
      FROM `tractian-bi.pepeuhacker.hubspot_deals_live`
    )
    SELECT
      DATE('{day_iso}'),
      CURRENT_TIMESTAMP(),
      (SELECT source_at FROM fonte),
      d.deal_id,
      d.deal_name,
      d.account_executive,
      d.deal_type,
      h.dealstage,
      SAFE_CAST(d.amount AS FLOAT64),
      DATE(h.closedate),
      h.hs_manual_forecast_category,
      d.po_emission_date,
      d.pipeline_label
    FROM `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
    JOIN `tractian-bi.pepeuhacker.hubspot_deals_live` h
      ON CAST(h.dealId AS STRING) = d.deal_id
    WHERE d.pipeline_label = 'Sales BR'
      AND d.account_executive IS NOT NULL
      AND ( h.dealstage IN ({_stages})
            OR (h.dealstage = '{lost_stage_id}'
                AND DATE(h.closedate) >= DATE_SUB(DATE('{day_iso}'), INTERVAL 200 DAY)) )
      AND NOT EXISTS (
        SELECT 1 FROM `{FC_DAILY_TABLE}` s
        WHERE s.snapshot_date = DATE('{day_iso}') AND s.pipeline = 'Sales BR'
      )
    """
    # Mesma tradução de toda query compartilhada: troca o funil E os ids de
    # estágio (os abertos, won e lost) pro vocabulário do pipeline. A foto guarda
    # o id CRU e a moeda do deal; a leitura normaliza e converte.
    sql = _apply_pipeline(sql, pipeline)
    job = client().query(sql)
    job.result()
    return int(job.num_dml_affected_rows or 0)


def forecast_daily_snapshots(pipeline: str = SALES_BR,
                             force_refresh: bool = False) -> pd.DataFrame:
    """Que meias-noites têm foto própria NESTE funil: uma linha por dia, com o
    instante da captura e a idade do dado congelado. Por funil porque o México
    só começou a ser fotografado em 13/10/2026 — um dia com foto do Brasil não é
    um dia com foto do México."""
    sql = f"""
    SELECT FORMAT_DATE('%Y%m%d', snapshot_date) AS dia,
           MIN(captured_at)                     AS captured_at,
           MIN(source_at)                       AS source_at,
           COUNT(*)                             AS n
    FROM `{FC_DAILY_TABLE}`
    WHERE pipeline = '{pipeline}'
    GROUP BY 1
    """
    return _query_raw(sql, cache_key=_pk("fc_daily_snapshots_v2", pipeline), ttl=600,
                      force_refresh=force_refresh)


def forecast_daily_series(dias: list[str], lost_from: str | None = None,
                          lost_to: str | None = None, pipeline: str = SALES_BR,
                          force_refresh: bool = False) -> pd.DataFrame:
    """Mesmo shape de `forecast_asof_series` (uma linha por deal × dia), lido das
    fotos da meia-noite. `dias` em YYYYMMDD. Se o dia tiver mais de uma captura
    (corrida entre instâncias), fica a MAIS CEDO — a mais perto da virada."""
    if not dias:
        return pd.DataFrame(columns=["dia", "deal_id", "deal_name", "account_executive",
                                     "deal_type", "stage", "amount", "close_date",
                                     "forecast_category", "po_emission_date"])
    _in = ", ".join(f"PARSE_DATE('%Y%m%d', '{x}')" for x in dias)
    _lost = (f"AND NOT (stage = 'closedlost' AND close_date NOT BETWEEN "
             f"'{lost_from}' AND '{lost_to}')" if lost_from and lost_to else "")
    sql = f"""
    SELECT FORMAT_DATE('%Y%m%d', snapshot_date) AS dia, deal_id, deal_name,
           account_executive, deal_type, stage, amount, close_date,
           forecast_category, po_emission_date
    FROM `{FC_DAILY_TABLE}`
    WHERE snapshot_date IN ({_in}) AND pipeline = 'Sales BR' {_lost}
    QUALIFY captured_at = MIN(captured_at) OVER (PARTITION BY snapshot_date)
    """
    key = "fc_daily_v2_" + hashlib.md5(
        ("|".join(dias) + f"#{lost_from}:{lost_to}").encode()).hexdigest()[:16]
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk(key, pipeline), ttl=86400,
                 force_refresh=force_refresh)


def pipeline_stalled(pipeline: str = SALES_BR, force_refresh: bool = False) -> pd.DataFrame:
    """Deals abertos no funil Sales BR ATUALMENTE em algum dos 5 stages que têm
    SLA de inatividade — devolve estado atual (stage + dias desde a última
    transição). O caller filtra por threshold de cada stage:
      Qualification    (contractsent)          → 14d
      Project Scoping  (93878633)              → 21d
      Negotiation      (decisionmakerboughtin) → 14d
      Consensus        (87188634)              → 14d
      Procurement      (65917881)              → 21d
    "Dias parado" = hoje − data de ENTRADA no estágio atual (hs_v2_date_entered_
    current_stage, via pepeuhacker.hubspot_deals_stage_live), em dias UTC. O
    contador zera a cada reentrada no estágio, inclusive ida-e-volta no mesmo
    dia (conferido em 09/10/2026: em 736 deals bate com o histórico em 733; os
    3 restantes reentraram depois que o histórico parou, em 01/10).
    Até 08/10/2026 vinha do último item de `hubspot_deals_history`."""
    sql = f"""
    WITH cur AS (
      SELECT dealId, stage AS stage_curr, changed_at AS stage_changed_at
      FROM ({_stage_snapshot_sql(None, pipeline)})
    ),
    ae_meta AS (
      SELECT account_executive, leader, region
      FROM `tractian-bi.dbt_analytics_dev.account_executives_performance`
      WHERE region IS NOT NULL AND role = 'AE'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY account_executive ORDER BY quarter DESC) = 1
    )
    SELECT
      d.deal_id,
      d.deal_name,
      d.account_executive,
      ae.leader,
      ae.region,
      COALESCE(SAFE_CAST(d.amount AS FLOAT64), 0) AS amount,
      cur.stage_curr,
      cur.stage_changed_at,
      DATE_DIFF(CURRENT_DATE(), DATE(cur.stage_changed_at), DAY) AS days_stalled
    FROM cur
    JOIN `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
      ON CAST(d.deal_id AS INT64) = cur.dealId
    LEFT JOIN ae_meta ae USING (account_executive)
    WHERE d.pipeline_label = 'Sales BR'
      AND d.account_executive IS NOT NULL
      AND cur.stage_curr IN ('contractsent', '93878633',
                             'decisionmakerboughtin', '87188634', '65917881')
      AND cur.stage_changed_at IS NOT NULL
    """
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk("pipeline_stalled_v3", pipeline), ttl=600, force_refresh=force_refresh)


def pipeline_stalled_wow(t_curr_iso: str, t_prev_iso: str,
                         pipeline: str = SALES_BR, force_refresh: bool = False) -> pd.DataFrame:
    """Estado de cada deal nos 2 snapshots (t_curr, t_prev) — usado pra calcular
    quem entrou/saiu da inatividade WoW. Universo: deals do funil Sales BR
    que tocam algum dos 5 stages com SLA em pelo menos um dos snapshots.

    Retorna por deal:
      stage_curr, changed_at_curr, days_curr
      stage_prev, changed_at_prev, days_prev"""
    sql = f"""
    WITH sc AS ({_stage_snapshot_sql(t_curr_iso, pipeline)}),
    sp AS ({_stage_snapshot_sql(t_prev_iso, pipeline)}),
    snaps AS (
      SELECT dealId, sc.stage AS stage_curr, sc.changed_at AS changed_at_curr,
             sp.stage AS stage_prev, sp.changed_at AS changed_at_prev
      FROM sc FULL OUTER JOIN sp USING (dealId)
    ),
    ae_meta AS (
      SELECT account_executive, leader, region
      FROM `tractian-bi.dbt_analytics_dev.account_executives_performance`
      WHERE region IS NOT NULL AND role = 'AE'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY account_executive ORDER BY quarter DESC) = 1
    )
    SELECT
      d.deal_id,
      d.deal_name,
      d.account_executive,
      ae.leader,
      ae.region,
      COALESCE(SAFE_CAST(d.amount AS FLOAT64), 0) AS amount,
      snaps.stage_curr,
      snaps.changed_at_curr,
      DATE_DIFF(DATE(TIMESTAMP('{t_curr_iso}')), DATE(snaps.changed_at_curr), DAY) AS days_curr,
      snaps.stage_prev,
      snaps.changed_at_prev,
      DATE_DIFF(DATE(TIMESTAMP('{t_prev_iso}')), DATE(snaps.changed_at_prev), DAY) AS days_prev
    FROM snaps
    JOIN `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
      ON CAST(d.deal_id AS INT64) = snaps.dealId
    LEFT JOIN ae_meta ae USING (account_executive)
    WHERE d.pipeline_label = 'Sales BR'
      AND d.account_executive IS NOT NULL
      AND (snaps.stage_curr IN ('contractsent','93878633','decisionmakerboughtin','87188634','65917881')
        OR snaps.stage_prev IN ('contractsent','93878633','decisionmakerboughtin','87188634','65917881'))
    """
    # v2 = +deal_name/amount (drill da movimentação WoW da página Stale)
    key = f"pipe_stalled_wow3_{t_prev_iso}_{t_curr_iso}".replace(" ", "_").replace(":", "")
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk(key, pipeline), ttl=86400, force_refresh=force_refresh)


# ---------------------------------------------------------------------------
# Inatividade por ATIVIDADE (V2) — mede dias desde o último RETORNO de um contato
# do deal, não dias no stage. "Retorno" = engagement de duas vias, atrelado ao
# deal via a ponte contato↔deal (hubspot_vid_associated_with_deal_id):
#   • email  : INCOMING_EMAIL (resposta do contato), exceto out-of-office
#   • call   : INBOUND (contato ligou) OU outbound conectada (duração ≥ 30s)
#   • gong   : reunião gravada pelo Gong com um contato do deal (desde 19/10/2026;
#              view pepeuhacker.gong_meeting_contacts_live — sql/). Antes meeting
#              não entrava: a hubspot_meetings do BQ não traz associação, mas traz
#              o e-mail dos participantes, e o conector traz o resto.
# `_ACTIVITY_LOOKBACK_DAYS` limita o scan das tabelas gigantes de engagement:
# atividade mais velha que isso já estoura qualquer SLA, então cai no fallback
# (stage_changed_at) sem custo de varrer o histórico inteiro.
_ACTIVITY_LOOKBACK_DAYS = 365

_STALLED_STAGE_IDS_SQL = (
    "'contractsent','93878633','decisionmakerboughtin','87188634','65917881'")
# Inatividade (dias sem retorno) cobre também o Stage 1 - Scheduled desde
# 09/10/2026; o Stale (dias no estágio) segue só com os 5 de SLA acima.
_ACTIVITY_STAGE_IDS_SQL = "'appointmentscheduled'," + _STALLED_STAGE_IDS_SQL


# ── Estágio da inatividade: fonte viva, não o histórico ──────────────────────
# `hubspot.hubspot_deals_history` parou em 2026-10-01 23:49 UTC (08/10/2026): as
# telas de Inatividade e Stale passaram a mostrar deal já fechado como parado e
# estágio velho com o SLA errado. O estado ATUAL vem agora da view
# pepeuhacker.hubspot_deals_stage_live (sql/hubspot_deals_stage_live.sql), com a
# data de entrada no estágio que o próprio HubSpot guarda. Os snapshots de
# domingo do WoW usam o histórico enquanto ele cobre a data; depois disso, a
# cópia diária do BI (estágio) + a data de entrada por estágio da view viva.
_STAGE_LIVE = "tractian-bi.pepeuhacker.hubspot_deals_stage_live"
_SLA_STAGE_IDS = ("appointmentscheduled", "contractsent", "93878633", "decisionmakerboughtin",
                  "87188634", "65917881")
_HIST_MAX_CACHE: dict = {}
_COPY_EXISTS_CACHE: dict = {}


def _hist_max():
    """Último timestamp de mudança de estágio no histórico (cache de 1h)."""
    import time as _t
    hit = _HIST_MAX_CACHE.get("v")
    if hit and _t.time() - hit[1] < 3600:
        return hit[0]
    try:
        r = list(client().query("""SELECT MAX(s.timestamp) m
            FROM `tractian-bi.pepeuhacker.hubspot_deals_history_live` h, UNNEST(h.dealstage) s""").result())
        v = pd.Timestamp(r[0]["m"]) if r and r[0]["m"] is not None else None
    except Exception:
        v = None
    _HIST_MAX_CACHE["v"] = (v, _t.time())
    return v


def _copy_table(d) -> str | None:
    """Cópia diária da curated do dia `d` (materialized_data_source), se existir."""
    name = f"tractian-bi.materialized_data_source.int_adding_names_to_deals{d:%Y%m%d}"
    if name not in _COPY_EXISTS_CACHE:
        try:
            client().get_table(name)
            _COPY_EXISTS_CACHE[name] = True
        except Exception:
            _COPY_EXISTS_CACHE[name] = False
    return name if _COPY_EXISTS_CACHE[name] else None


def _entered_case(stage_expr: str, alias: str, pipeline: str) -> str:
    """CASE estágio → data de entrada NAQUELE estágio (colunas da view viva).
    O literal do WHEN fica no id do BR (o _apply_pipeline traduz); a COLUNA já
    sai no id do pipeline alvo, porque o _apply_pipeline só mexe em literal."""
    m = _SQL_STAGE_MAP.get(pipeline, {})
    whens = " ".join(f"WHEN '{sid}' THEN {alias}.hs_v2_date_entered_{m.get(sid, sid)}"
                     for sid in _SLA_STAGE_IDS)
    return f"(CASE {stage_expr} {whens} END)"


def _stage_snapshot_sql(asof_iso: str | None, pipeline: str = SALES_BR) -> str:
    """SELECT (dealId INT64, stage, changed_at) — estágio e data de entrada
    nele, agora (asof_iso=None) ou num instante passado."""
    if asof_iso is None:
        # 2 deals em Scheduled vêm sem a data de entrada (09/10/2026): no Stage 1
        # ela é a própria criação do deal.
        return f"""SELECT SAFE_CAST(dealId AS INT64) AS dealId, dealstage AS stage,
                   COALESCE(stage_entered_at, createdate) AS changed_at FROM `{_STAGE_LIVE}`"""
    t = pd.Timestamp(asof_iso)
    hm = _hist_max()
    if hm is not None and t <= hm:
        return f"""SELECT h.dealId,
            (SELECT s.value FROM UNNEST(h.dealstage) s WHERE s.timestamp <= TIMESTAMP('{asof_iso}')
               ORDER BY s.timestamp DESC LIMIT 1) AS stage,
            (SELECT s.timestamp FROM UNNEST(h.dealstage) s WHERE s.timestamp <= TIMESTAMP('{asof_iso}')
               ORDER BY s.timestamp DESC LIMIT 1) AS changed_at
          FROM `tractian-bi.pepeuhacker.hubspot_deals_history_live` h"""
    cp = _copy_table(t.date())
    if cp is None:
        # Sem cópia do dia: melhor o histórico (defasado) do que nada — e avisa.
        print(f"bq · AVISO: snapshot {asof_iso} sem cópia diária; usando histórico até {hm}", flush=True)
        return _stage_snapshot_sql(str(hm), pipeline) if hm is not None else _stage_snapshot_sql(None, pipeline)
    case = _entered_case("c.deal_stage_id", "l", pipeline)
    return f"""SELECT SAFE_CAST(c.deal_id AS INT64) AS dealId, c.deal_stage_id AS stage,
          COALESCE(IF({case} <= TIMESTAMP('{asof_iso}'), {case}, NULL),
                   TIMESTAMP(c.date_entered_current_stage)) AS changed_at
        FROM `{cp}` c
        LEFT JOIN `{_STAGE_LIVE}` l ON l.dealId = CAST(c.deal_id AS STRING)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY c.deal_id ORDER BY c.deal_stage_id) = 1"""


def _activity_cte(asof_iso: str | None, sfx: str = "") -> tuple[str, str]:
    """Constrói as CTEs do último retorno de contato por deal e devolve
    (sql_das_ctes, nome_da_cte_final). `sfx` sufixa os nomes das CTEs pra poder
    montar dois snapshots (curr/prev) no mesmo WITH sem colisão. asof_iso=None →
    estado atual [now-lookback, now]; senão, snapshot histórico [asof-lookback, asof]."""
    hi = f"TIMESTAMP('{asof_iso}')" if asof_iso else "CURRENT_TIMESTAMP()"
    lo = f"TIMESTAMP_SUB({hi}, INTERVAL {_ACTIVITY_LOOKBACK_DAYS} DAY)"
    br, em, ca, wa, gm, ac, la = (f"bridge{sfx}", f"email_ret{sfx}", f"call_ret{sfx}",
                                  f"wa_ret{sfx}", f"gong_ret{sfx}", f"acts{sfx}", f"last_act{sfx}")
    cte = f"""
    {br} AS (
      SELECT CAST(dealId AS STRING) AS deal_id, CAST(vid AS STRING) AS vid
      FROM `tractian-bi.hubspot.hubspot_vid_associated_with_deal_id`
    ),
    {em} AS (
      SELECT b.deal_id, SAFE_CAST(e.properties.hs_timestamp AS TIMESTAMP) AS ts, 'email' AS ch
      FROM `tractian-bi.hubspot_nekt.hubspot_emails` e
      JOIN {br} b ON CAST(e.properties.hs_email_associated_contact_id AS STRING) = b.vid
      WHERE e.properties.hs_email_direction = 'INCOMING_EMAIL'
        AND COALESCE(e.properties.hs_incoming_email_is_out_of_office, 'false') != 'true'
        AND SAFE_CAST(e.properties.hs_timestamp AS TIMESTAMP) BETWEEN {lo} AND {hi}
    ),
    {ca} AS (
      SELECT b.deal_id, SAFE_CAST(c.properties.hs_timestamp AS TIMESTAMP) AS ts, 'call' AS ch
      FROM `tractian-bi.hubspot_nekt.hubspot_calls` c
      JOIN {br} b ON c.properties.hs_call_callee_object_type = 'CONTACT'
                 AND CAST(c.properties.hs_call_callee_object_id AS STRING) = b.vid
      WHERE (c.properties.hs_call_direction = 'INBOUND'
             OR SAFE_CAST(c.properties.hs_call_duration AS INT64) >= 30000)
        AND SAFE_CAST(c.properties.hs_timestamp AS TIMESTAMP) BETWEEN {lo} AND {hi}
    ),
    {wa} AS (
      -- WhatsApp: fala do CLIENTE numa nota do Sniper (integrador), gravada
      -- por tools/wa_ingest.py no grão de DIA UTC — ts = meia-noite do dia.
      -- No snapshot de domingo 15h a resposta das 18h do próprio domingo já
      -- conta; a diferença nunca passa de 1 dia e não move ninguém de SLA.
      -- Só conversa do DONO do deal (match = 'owner'): o HubSpot pendura a
      -- nota em todos os deals da empresa do contato, e sem o dono a conversa
      -- de um colega zerava a inatividade de deals que não eram dele.
      SELECT deal_id, TIMESTAMP(return_date) AS ts, 'whatsapp' AS ch
      FROM `tractian-bi.pepeuhacker.wa_deal_returns`
      WHERE return_date BETWEEN DATE({lo}) AND DATE({hi})
        AND match = 'owner'
    ),
    {gm} AS (
      -- Reunião gravada pelo Gong: conta pro deal quando um CONTATO associado ao
      -- deal participou (regra do Pedro, 19/10/2026) — qualquer pessoa da
      -- Tractian na reunião, não só o dono. O "deal do pipeline de vendas" é o
      -- filtro de pipeline da consulta que usa estas CTEs. Depois de 29/09 o
      -- grão é DIA UTC (carga pelo conector), como no WhatsApp.
      SELECT b.deal_id, g.ts, 'gong' AS ch
      FROM `tractian-bi.pepeuhacker.gong_meeting_contacts_live` g
      JOIN {br} b ON b.vid = g.contact_id
      WHERE g.ts BETWEEN {lo} AND {hi}
    ),
    {ac} AS (
      SELECT * FROM {em} UNION ALL SELECT * FROM {ca} UNION ALL SELECT * FROM {wa}
      UNION ALL SELECT * FROM {gm}
    ),
    {la} AS (
      -- Só atividade de DEPOIS da criação do deal: e-mail antigo do contato
      -- (anterior ao deal) deixava deal novo com 100+ dias "sem retorno" —
      -- pior do que se não houvesse atividade nenhuma (Kraft Heinz, 08/10/2026).
      SELECT a.deal_id,
        MAX(a.ts)                               AS last_activity_at,
        MAX(IF(a.ch = 'email', a.ts, NULL))     AS last_email_at,
        MAX(IF(a.ch = 'call',  a.ts, NULL))     AS last_call_at,
        MAX(IF(a.ch = 'whatsapp', a.ts, NULL))  AS last_whatsapp_at,
        MAX(IF(a.ch = 'gong', a.ts, NULL))      AS last_gong_at
      FROM {ac} a
      LEFT JOIN (SELECT dealId AS deal_id, createdate FROM `{_STAGE_LIVE}`) cr USING (deal_id)
      WHERE a.ts IS NOT NULL AND (cr.createdate IS NULL OR DATE(a.ts) >= DATE(cr.createdate))
      GROUP BY a.deal_id
    )"""
    return cte, la


def pipeline_activity(pipeline: str = SALES_BR, force_refresh: bool = False) -> pd.DataFrame:
    """Deals abertos no funil Sales BR ATUALMENTE nos 5 stages com SLA — devolve,
    por deal, o stage atual e a data do último RETORNO de um contato (email
    entrante / call conectada / fala do cliente no WhatsApp / reunião gravada
    no Gong). `days_since_activity` = dias desde esse retorno;
    quando nunca houve retorno (na janela), cai no `stage_changed_at` como origem
    justa (não pune deal recém-chegado ao stage). `has_activity` diferencia os
    dois casos. Espelha pipeline_stalled(), trocando o eixo de tempo."""
    act_cte, la = _activity_cte(None)
    sql = f"""
    WITH cur AS (
      SELECT dealId, stage AS stage_curr, changed_at AS stage_changed_at
      FROM ({_stage_snapshot_sql(None, pipeline)})
    ),
    ae_meta AS (
      SELECT account_executive, leader, region
      FROM `tractian-bi.dbt_analytics_dev.account_executives_performance`
      WHERE region IS NOT NULL AND role = 'AE'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY account_executive ORDER BY quarter DESC) = 1
    ),
    {act_cte}
    SELECT
      d.deal_id,
      d.deal_name,
      d.account_executive,
      ae.leader,
      ae.region,
      COALESCE(SAFE_CAST(d.amount AS FLOAT64), 0) AS amount,
      cur.stage_curr,
      cur.stage_changed_at,
      la.last_activity_at,
      la.last_email_at,
      la.last_call_at,
      la.last_whatsapp_at,
      la.last_gong_at,
      (la.last_activity_at IS NOT NULL) AS has_activity,
      DATE_DIFF(CURRENT_DATE(),
                DATE(COALESCE(la.last_activity_at, cur.stage_changed_at)), DAY)
        AS days_since_activity
    FROM cur
    JOIN `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
      ON CAST(d.deal_id AS INT64) = cur.dealId
    LEFT JOIN ae_meta ae USING (account_executive)
    LEFT JOIN last_act la ON la.deal_id = CAST(d.deal_id AS STRING)
    WHERE d.pipeline_label = 'Sales BR'
      AND d.account_executive IS NOT NULL
      AND cur.stage_curr IN ({_ACTIVITY_STAGE_IDS_SQL})
      AND cur.stage_changed_at IS NOT NULL
    """
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk("pipeline_activity_v6", pipeline), ttl=600, force_refresh=force_refresh)


def pipeline_activity_wow(t_curr_iso: str, t_prev_iso: str,
                          pipeline: str = SALES_BR, force_refresh: bool = False) -> pd.DataFrame:
    """Estado de atividade de cada deal em 2 snapshots (domingos), pra calcular
    quem saiu/entrou da inatividade-por-atividade WoW. Por snapshot: stage as-of
    (do histórico) + último retorno de contato as-of + days_since as-of. Mesmo
    universo/padrão de pipeline_stalled_wow()."""
    def _snap(hi_iso, suffix):
        return f"""
        stage_{suffix} AS ({_stage_snapshot_sql(hi_iso, pipeline)})"""
    act_curr, la_c = _activity_cte(t_curr_iso, sfx="_c")
    act_prev, la_p = _activity_cte(t_prev_iso, sfx="_p")
    sql = f"""
    WITH
    {_snap(t_curr_iso, 'curr')},
    {_snap(t_prev_iso, 'prev')},
    ae_meta AS (
      SELECT account_executive, leader, region
      FROM `tractian-bi.dbt_analytics_dev.account_executives_performance`
      WHERE region IS NOT NULL AND role = 'AE'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY account_executive ORDER BY quarter DESC) = 1
    ),
    {act_curr},
    {act_prev}
    SELECT
      d.deal_id,
      d.deal_name,
      d.account_executive,
      ae.leader,
      ae.region,
      COALESCE(SAFE_CAST(d.amount AS FLOAT64), 0) AS amount,
      sc.stage AS stage_curr,
      DATE_DIFF(DATE(TIMESTAMP('{t_curr_iso}')),
                DATE(COALESCE(lac.last_activity_at, sc.changed_at)), DAY) AS days_curr,
      sp.stage AS stage_prev,
      DATE_DIFF(DATE(TIMESTAMP('{t_prev_iso}')),
                DATE(COALESCE(lap.last_activity_at, sp.changed_at)), DAY) AS days_prev
    FROM stage_curr sc
    JOIN stage_prev sp USING (dealId)
    JOIN `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
      ON CAST(d.deal_id AS INT64) = sc.dealId
    LEFT JOIN ae_meta ae USING (account_executive)
    LEFT JOIN {la_c} lac ON lac.deal_id = CAST(d.deal_id AS STRING)
    LEFT JOIN {la_p} lap ON lap.deal_id = CAST(d.deal_id AS STRING)
    WHERE d.pipeline_label = 'Sales BR'
      AND d.account_executive IS NOT NULL
      AND (sc.stage IN ({_ACTIVITY_STAGE_IDS_SQL})
        OR sp.stage IN ({_ACTIVITY_STAGE_IDS_SQL}))
    """
    # v2 = +deal_name/amount (drill da movimentação WoW da página Stale · Atividade)
    # v3 = + WhatsApp (pepeuhacker.wa_deal_returns) entre os canais de retorno
    # wow7 = + reunião do Gong (pepeuhacker.gong_meeting_contacts_live)
    key = f"pipe_activity_wow7_{t_prev_iso}_{t_curr_iso}".replace(" ", "_").replace(":", "")
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk(key, pipeline), ttl=86400, force_refresh=force_refresh)


def gong_freshness(force_refresh: bool = False) -> pd.DataFrame:
    """Até que dia as reuniões do Gong estão carregadas. Depois de 29/09/2026 elas
    só chegam pela carga do conector (tools/gong_ingest.py, rotina semanal no
    Mac do Pedro), dia a dia: `until_date` = último dia completo, `first_gap` =
    primeiro dia faltando desde 29/09 (None se não há buraco) — com buraco, o
    "lido até" honesto é a véspera dele."""
    sql = """
    WITH ok AS (
      SELECT d.meeting_date
      FROM `tractian-bi.pepeuhacker.gong_days` d
      LEFT JOIN `tractian-bi.pepeuhacker.gong_day_searches` s USING (meeting_date)
      GROUP BY d.meeting_date, d.n_searches
      HAVING COUNT(DISTINCT s.search_idx) >= d.n_searches
    ),
    cal AS (
      SELECT d FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2026-09-29',
                                               (SELECT MAX(meeting_date) FROM ok))) d
    )
    SELECT
      (SELECT MAX(meeting_date) FROM ok) AS until_date,
      (SELECT MIN(cal.d) FROM cal LEFT JOIN ok ON ok.meeting_date = cal.d
        WHERE ok.meeting_date IS NULL) AS first_gap,
      (SELECT MAX(loaded_at) FROM `tractian-bi.pepeuhacker.gong_days`) AS loaded_at
    """
    return query(sql, cache_key="gong_freshness_v1", ttl=600, force_refresh=force_refresh)


def wa_freshness(force_refresh: bool = False) -> pd.DataFrame:
    """Frescor da carga de WhatsApp (tools/wa_ingest.py): até quando as notas
    foram lidas e quando foi a última associação a deal. A carga roda numa
    rotina diária do Claude (o app não tem token do HubSpot), então ela PODE
    atrasar — a tela mostra a data pra ninguém ler um "sem retorno" velho como
    inatividade de verdade."""
    sql = """
    SELECT
      (SELECT MAX(modified_at) FROM `tractian-bi.pepeuhacker.wa_notes`)       AS notes_until,
      (SELECT MAX(mapped_at)   FROM `tractian-bi.pepeuhacker.wa_deal_returns`) AS mapped_at,
      (SELECT MAX(return_date) FROM `tractian-bi.pepeuhacker.wa_deal_returns`) AS last_return_date,
      (SELECT MIN(return_date) FROM `tractian-bi.pepeuhacker.wa_deal_returns`) AS first_return_date
    """
    return query(sql, cache_key="wa_freshness_v1", ttl=600, force_refresh=force_refresh)


def email_call_freshness(force_refresh: bool = False) -> pd.DataFrame:
    """Último e-mail e última ligação que chegaram ao BigQuery (nekt), sem olhar
    direção nem duração — a pergunta aqui é se o SYNC está vivo, não se houve
    retorno. 12/10/2026: as duas tabelas pararam em 29/09 (calls) e 01/10
    (emails) sem nenhuma substituta no BQ, e a Inatividade inflou calada: todo
    retorno por esses canais depois disso ficou invisível."""
    sql = """
    SELECT
      (SELECT MAX(SAFE_CAST(properties.hs_timestamp AS TIMESTAMP))
         FROM `tractian-bi.hubspot_nekt.hubspot_emails`
        WHERE SAFE_CAST(properties.hs_timestamp AS TIMESTAMP) <= CURRENT_TIMESTAMP()) AS email_until,
      (SELECT MAX(SAFE_CAST(properties.hs_timestamp AS TIMESTAMP))
         FROM `tractian-bi.hubspot_nekt.hubspot_calls`
        WHERE SAFE_CAST(properties.hs_timestamp AS TIMESTAMP) <= CURRENT_TIMESTAMP()) AS call_until
    """
    return query(sql, cache_key="email_call_freshness_v1", ttl=3600, force_refresh=force_refresh)


def tickets_freshness(force_refresh: bool = False) -> pd.DataFrame:
    """Último ticket criado que chegou ao BigQuery. 12/10/2026: `hubspot.hubspot_tickets`
    (e as associações a deal) parou de sincronizar em 01/10 e não há substituta
    viva no BQ — toda a cadeia do BI (`views.stg_hubspot__tickets` →
    `int_adding_names_to_tickets`) lê a mesma tabela. A aba Legal avisa."""
    sql = """
    SELECT MAX(TIMESTAMP(createdate)) AS ticket_until
    FROM `tractian-bi.hubspot.hubspot_tickets`
    WHERE TIMESTAMP(createdate) <= CURRENT_TIMESTAMP()
    """
    return query(sql, cache_key="tickets_freshness_v1", ttl=3600, force_refresh=force_refresh)


def confirm_totals(quarter_from: str, quarter_to: str,
                   force_refresh: bool = False) -> pd.DataFrame:
    """Source of truth for Gross totals per region × quarter, usado pra validar
    os números do dashboard. Cache de 5min — ?refresh=1 força re-query.
    (Antes era 'no cache' e cobrava ~11s por hit; dbt roda poucas vezes ao dia,
    então TTL curto não compromete a semântica de fonte-da-verdade.)"""
    sql = f"""
    SELECT
      region,
      quarter,
      COUNT(DISTINCT CASE WHEN new_mrr_in_brl > 0 THEN account_executive END) AS aes_com_venda,
      ROUND(SUM(new_mrr_in_brl), 0)            AS gross_mrr_brl,
      ROUND(SUM(new_mrr_in_brl_first_po), 0)   AS gross_acq_brl,
      ROUND(SUM(new_mrr_in_brl_expansion), 0)  AS gross_exp_brl,
      ROUND(SUM(net_mrr_in_brl), 0)            AS net_mrr_brl,
      ROUND(SUM(new_mrr_in_brl) - SUM(net_mrr_in_brl), 0) AS churn_impact_brl,
      ROUND(SUM(quota_in_brl), 0)              AS quota_brl,
      SUM(deals_won)                           AS deals_won
    FROM `tractian-bi.dbt_analytics_dev.account_executives_performance`
    WHERE quarter BETWEEN DATE '{quarter_from}' AND DATE '{quarter_to}'
      AND region IS NOT NULL
    GROUP BY region, quarter
    ORDER BY region, quarter
    """
    return query(sql, cache_key=f"confirm_totals_{quarter_from}_{quarter_to}",
                 ttl=300, force_refresh=force_refresh)


def sold_total_live(quarter_start: str, quarter_end: str,
                    region: str | None = None) -> float:
    """Always-live total sold value (source of truth). Bypasses cache."""
    where_region = f"AND region = '{region}'" if region else ""
    sql = f"""
    SELECT COALESCE(SUM(new_mrr_in_brl), 0) AS total_sold_brl
    FROM `tractian-bi.dbt_analytics_dev.account_executives_performance`
    WHERE quarter BETWEEN DATE '{quarter_start}' AND DATE '{quarter_end}'
    {where_region}
    """
    return float(client().query(sql).to_dataframe().iloc[0]["total_sold_brl"])


# ============================================================
# Aba Legal — jurídico envolvido no deal (tickets + interação nominal)
# ============================================================
# Pipelines de ticket do jurídico no HubSpot. São dois porque o time atende BR e
# NA na MESMA estrutura de estágios (mesmo stageOrder, mesmos rótulos): manter os
# dois na lista deixa a aba honesta pra um deal Sales BR cujo contrato foi tocado
# pelo pipeline da América do Norte, sem inventar uma 3ª categoria.
LEGAL_PIPELINES = ("77965236", "123809559")   # Legal BR · Legal North America

# Identidades do jurídico. `user_id` é o id de USUÁRIO do HubSpot (o que aparece
# em hs_pipeline_stage.updatedByUserId no histórico do ticket); `email` é o que
# casa no tráfego de e-mail. juridico@ não é usuário nem owner do HubSpot — só
# existe como caixa, então pra ele só há o canal de e-mail.
LEGAL_PEOPLE = (
    {"key": "ana",      "label": "Ana Meneguelli", "email": "ameneguelli@tractian.com", "user_id": 62484138},
    {"key": "luiza",    "label": "Luiza Daudt",    "email": "ldaudt@tractian.com",      "user_id": 85272521},
    {"key": "juridico", "label": "juridico@",      "email": "juridico@tractian.com",    "user_id": None},
)


def legal_deals_raw(pipeline: str = SALES_BR, force_refresh: bool = False) -> pd.DataFrame:
    """Uma linha por deal do Sales BR com o estado do JURÍDICO ao lado do
    desfecho comercial. Base da aba Legal.

    Três blocos de informação por deal:

    1. Desfecho e trajetória — `dealstage`/`close_date` vêm do RAW
       (`pepeuhacker.hubspot_deals_live`) pelo mesmo motivo já documentado em
       `pipeline_deals_raw`: a curada devolve close_date NULL pra closedlost e
       confunde "aberto" com "perdido sem data". `max_stage_num` é o estágio
       MAIS AVANÇADO em que o deal já pisou, reconstruído do array `dealstage`
       do histórico — é o que permite comparar maçã com maçã na aba (ver abaixo).

    2. Ticket do jurídico — quantos tickets dos pipelines LEGAL_PIPELINES estão
       associados ao deal, quando o primeiro foi aberto e onde eles pararam.
       O estágio não vira um "máximo" simples porque Lost tem stageOrder 7, ACIMA
       de Signed (5): ordenar por número diria que um contrato perdido avançou
       mais que um assinado. Por isso vêm três medidas independentes —
       `legal_signed` (Signed/concluded), `legal_lost` (Lost) e
       `legal_open_order` (o mais avançado entre Requested..Sent to Signature) —
       e quem decide o rótulo é o app.

    3. Toque nominal no ticket — se Ana ou Luiza mexeram no estágio do ticket
       (`updatedByUserId` no histórico). Hoje só a Ana aparece por esse canal;
       a Luiza atua por e-mail, que é o que `legal_mail_touch_raw` cobre.

    Sobre o VIÉS que a aba precisa mostrar: o jurídico é acionado no fim do
    funil, quando o deal já está praticamente fechando. Comparar "com jurídico"
    contra TODO o resto da base mede a ordem dos eventos, não o efeito do
    jurídico — dá 84% de conversão contra 6%. `max_stage_num` existe pra a aba
    poder restringir a comparação a quem chegou em Negociação (4) ou além, onde
    acionar o jurídico era uma escolha real: aí a diferença cai pra ~86% contra
    ~27%, que é o número defensável."""
    sql = f"""
    WITH
    stg AS (
      SELECT pipelineId, stageId, stageOrder
      FROM `tractian-bi.hubspot.hubspot_ticket_stage_id`
      WHERE pipelineId IN {LEGAL_PIPELINES}
    ),
    lt AS (
      SELECT t.hs_object_id AS ticket_id, t.createdate, s.stageOrder
      FROM `tractian-bi.hubspot.hubspot_tickets` t
      JOIN stg s ON s.pipelineId = t.hs_pipeline AND s.stageId = t.hs_pipeline_stage
    ),
    -- DISTINCT: a tabela de associação repete o par (ticket, deal), e sem isso o
    -- COUNT(*) de tickets por deal contaria a mesma minuta várias vezes.
    assoc AS (
      SELECT DISTINCT CAST(dealId AS STRING) AS deal_id, CAST(ticketId AS STRING) AS ticket_id
      FROM `tractian-bi.hubspot.hubspot_tickets_associated_with_deals`
    ),
    touch AS (
      SELECT h.id AS ticket_id,
        LOGICAL_OR(s.updatedByUserId = {LEGAL_PEOPLE[0]['user_id']}) AS by_ana,
        LOGICAL_OR(s.updatedByUserId = {LEGAL_PEOPLE[1]['user_id']}) AS by_luiza
      FROM `tractian-bi.hubspot.hubspot_tickets_history` h, UNNEST(h.hs_pipeline_stage) s
      GROUP BY 1
    ),
    tk AS (
      SELECT a.deal_id,
        COUNT(*)                                                    AS legal_tickets,
        MIN(lt.createdate)                                          AS legal_first_at,
        COUNTIF(lt.stageOrder IN (5, 6))                            AS legal_signed,
        COUNTIF(lt.stageOrder = 7)                                  AS legal_lost,
        MAX(IF(lt.stageOrder BETWEEN 0 AND 4, lt.stageOrder, NULL)) AS legal_open_order,
        LOGICAL_OR(COALESCE(tc.by_ana, FALSE))                      AS tk_ana,
        LOGICAL_OR(COALESCE(tc.by_luiza, FALSE))                    AS tk_luiza
      FROM assoc a
      JOIN lt ON lt.ticket_id = a.ticket_id
      LEFT JOIN touch tc ON tc.ticket_id = a.ticket_id
      GROUP BY 1
    ),
    reach AS (
      SELECT CAST(h.dealId AS STRING) AS deal_id,
        MAX(CASE s.value WHEN 'appointmentscheduled'  THEN 1
                         WHEN 'contractsent'          THEN 2
                         WHEN '93878633'              THEN 3
                         WHEN 'decisionmakerboughtin' THEN 4
                         WHEN '87188634'              THEN 5
                         WHEN '65917881'              THEN 6 END) AS max_stage_num
      FROM `tractian-bi.pepeuhacker.hubspot_deals_history_live` h, UNNEST(h.dealstage) s
      GROUP BY 1
    )
    SELECT
      d.deal_id,
      d.deal_name,
      d.account_executive,
      d.deal_type,
      d.deal_source_agg,
      SAFE_CAST(d.amount AS FLOAT64) AS amount,
      d.created_date,
      d.stage_label,
      h.dealstage,
      DATE(h.closedate) AS close_date,
      COALESCE(reach.max_stage_num, 0) AS max_stage_num,
      COALESCE(tk.legal_tickets, 0)  AS legal_tickets,
      tk.legal_first_at,
      COALESCE(tk.legal_signed, 0)   AS legal_signed,
      COALESCE(tk.legal_lost, 0)     AS legal_lost,
      tk.legal_open_order,
      COALESCE(tk.tk_ana, FALSE)     AS tk_ana,
      COALESCE(tk.tk_luiza, FALSE)   AS tk_luiza
    FROM `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
    LEFT JOIN `tractian-bi.pepeuhacker.hubspot_deals_live` h
      ON CAST(h.dealId AS STRING) = d.deal_id
    LEFT JOIN tk    ON tk.deal_id = d.deal_id
    LEFT JOIN reach  ON reach.deal_id = d.deal_id
    WHERE d.pipeline_label = 'Sales BR'
      AND d.account_executive IS NOT NULL
    """
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk("legal_deals_raw_v2", pipeline), force_refresh=force_refresh)


# TTL longo (6h) porque esta é a query CARA da aba: ~25 GB varridos, contra ~0 GB
# de `legal_deals_raw`. A tabela de e-mails tem 199M linhas pra 1,2M mensagens
# reais (~168 versões de cada), então não há como recortá-la de leve. Com o TTL
# padrão de 10min uma tarde de navegação na aba custaria dezenas de varreduras;
# e-mail do jurídico num deal não muda de minuto em minuto, então 6h não perde
# nada de prático. Também de propósito FORA do `_warm_all`: o refresher roda de
# 45 em 45 min com force=True e transformaria isso em ~800 GB/dia.
LEGAL_MAIL_TTL = 6 * 3600


def legal_mail_touch_raw(pipeline: str = SALES_BR, force_refresh: bool = False) -> pd.DataFrame:
    """Interação POR E-MAIL do jurídico em cada deal do Sales BR: quantas
    mensagens têm Ana, Luiza ou juridico@ em qualquer campo de endereço
    (from/to/cc/bcc), e quando foi a primeira e a última.

    É o segundo canal de sinalização da aba Legal, e não é redundante com o
    ticket: 240 dos deals fechados que chegaram a Negociação+ têm e-mail do
    jurídico e NENHUM ticket aberto — jurídico acionado por fora do processo.

    Duas decisões que sustentam a precisão do sinal:

    `DISTINCT` no `ev`: a fonte é versionada (199M linhas / 1,2M mensagens), e
    contar sem deduplicar por `id` multiplicaria cada e-mail por ~168.

    Janela por VIDA DO DEAL: o vínculo e-mail→deal é indireto (e-mail → contato →
    deal, via `hubspot_vid_associated_with_deal_id`), e um contato costuma ter
    vários deals ao longo do tempo. Sem recorte temporal, a conversa jurídica de
    um contrato de 2024 aparece como "jurídico envolvido" no deal que o mesmo
    contato abriu em 2026 — o sinal cai de 3.251 deals pra 1.013 quando a janela
    entra, ou seja 2/3 do volume bruto era vazamento de outro deal. A janela vai
    da criação do deal até 30 dias depois do fechamento: a assinatura costuma
    escorrer alguns dias pra fora do close date, e cortar no dia exato perderia
    justamente o fim da negociação do contrato."""
    emails = ", ".join(f"'%{p['email']}%'" for p in LEGAL_PEOPLE)
    box = """CONCAT(COALESCE(e.properties.hs_email_from_email,''), ',',
                    COALESCE(e.properties.hs_email_to_email,''),   ',',
                    COALESCE(e.properties.hs_email_cc_email,''),   ',',
                    COALESCE(e.properties.hs_email_bcc_email,''))"""
    sql = f"""
    WITH
    dl AS (
      SELECT d.deal_id,
             TIMESTAMP(d.created_date) AS lo,
             TIMESTAMP_ADD(COALESCE(TIMESTAMP(DATE(h.closedate)), CURRENT_TIMESTAMP()),
                           INTERVAL 30 DAY) AS hi
      FROM `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
      LEFT JOIN `tractian-bi.pepeuhacker.hubspot_deals_live` h
        ON CAST(h.dealId AS STRING) = d.deal_id
      WHERE d.pipeline_label = 'Sales BR'
        AND d.account_executive IS NOT NULL
    ),
    bridge AS (
      SELECT DISTINCT CAST(dealId AS STRING) AS deal_id, CAST(vid AS STRING) AS vid
      FROM `tractian-bi.hubspot.hubspot_vid_associated_with_deal_id`
    ),
    ev AS (
      SELECT DISTINCT
        e.id,
        CAST(e.properties.hs_email_associated_contact_id AS STRING) AS vid,
        SAFE_CAST(e.properties.hs_timestamp AS TIMESTAMP) AS ts,
        LOWER({box}) AS box
      FROM `tractian-bi.hubspot_nekt.hubspot_emails` e
      WHERE e.properties.hs_email_associated_contact_id IS NOT NULL
        AND LOWER({box}) LIKE ANY ({emails})
    )
    SELECT
      dl.deal_id,
      COUNTIF(ev.box LIKE '%{LEGAL_PEOPLE[0]['email']}%') AS mail_ana,
      COUNTIF(ev.box LIKE '%{LEGAL_PEOPLE[1]['email']}%') AS mail_luiza,
      COUNTIF(ev.box LIKE '%{LEGAL_PEOPLE[2]['email']}%') AS mail_juridico,
      MIN(ev.ts) AS mail_first_at,
      MAX(ev.ts) AS mail_last_at
    FROM dl
    JOIN bridge b ON b.deal_id = dl.deal_id
    JOIN ev       ON ev.vid = b.vid AND ev.ts BETWEEN dl.lo AND dl.hi
    GROUP BY 1
    """
    sql = _apply_pipeline(sql, pipeline)
    return query(sql, pipeline=pipeline, cache_key=_pk("legal_mail_touch_v2", pipeline), ttl=LEGAL_MAIL_TTL,
                 force_refresh=force_refresh)


# ============================================================
# Sales MX — pipeline do México, com o valor já convertido em USD
# ============================================================
# Os deals do MX NÃO estão todos na mesma moeda: 6.532 em USD, 262 em MXN e 69
# em BRL (medido em 24/08/2026). Somar `amount` cru misturaria pesos e reais com
# dólares — daria um número que não significa nada. Por isso a conversão acontece
# AQUI, na origem, e a coluna que sai (`amount`) já é MRR em USD: nenhum consumidor
# precisa saber que existiu outra moeda, e não há como esquecer de converter numa
# tela nova.
#
# A taxa vem de `dbt_analytics_dev.currency_conversion_rates`, a mesma que o BI
# usa no resto da empresa — decisão do usuário em 24/08/2026, pra o dashboard não
# divergir dos relatórios oficiais.
#
# >>> RESSALVA IMPORTANTE, e ela é sobre o DADO, não sobre o código: essa tabela
# NÃO é uma série histórica. Ela tem duas `valid_date` (2024-12-01 e 2025-01-01)
# com taxas IDÊNTICAS — MXN 0,05 e BRL 0,20, ou seja 20 pesos e 5 reais por dólar.
# O join abaixo pega a cotação vigente na data do deal justamente para que, no dia
# em que o BI passar a alimentar a tabela por data, a conversão histórica comece a
# valer sozinha. Enquanto isso, ela é matematicamente uma taxa fixa — e é isso que
# a tela precisa dizer, em vez de sugerir que converteu na cotação do dia.
def mx_deals_raw(force_refresh: bool = False) -> pd.DataFrame:
    """Deals do funil Sales MX, uma linha por deal, com `amount` já em MRR USD.

    Espelha o que `pipeline_total_raw` + `forecast_deals_raw` fazem no BR, mas
    num só lugar: o MX é menor (6.863 deals) e não compensa varrer o histórico
    duas vezes. `amount_orig`/`deal_currency` ficam expostos pra a tela poder
    mostrar o valor como foi digitado no HubSpot quando alguém perguntar."""
    sql = """
    WITH fx AS (
      SELECT currency, conversion_usd, PARSE_DATE('%Y-%m-%d', valid_date) AS valid_date
      FROM `tractian-bi.dbt_analytics_dev.currency_conversion_rates`
      WHERE conversion_usd IS NOT NULL
    ),
    d AS (
      SELECT
        d.account_executive, d.deal_id, d.deal_name, d.deal_type,
        d.deal_source_agg, d.product_of_deal AS product,
        d.created_date, d.po_emission_date, d.stage_label,
        -- Datas de atividade/etapa: base das telas de Inatividade e Funil. Vêm
        -- junto e não numa 2ª query porque o MX é pequeno (6,8 mil deals) e uma
        -- varredura a mais custaria mais que as colunas.
        d.last_activity_date, d.date_entered_current_stage,
        d.first_meeting_eb, d.lost_reason, d.lost_date, d.sector,
        -- Datas de ENTRADA em cada etapa. São o que permite medir conversão
        -- honestamente: o estágio atual de um deal perdido é "Lost", então sem
        -- isso os 760 perdidos do MX sumiriam do denominador e a conversão de
        -- cada etapa apareceria muito melhor do que é.
        d.hs_date_entered_scheduled, d.hs_date_entered_qualification,
        d.hs_date_entered_project_scoping, d.hs_date_entered_negotiation,
        d.hs_date_entered_consensus, d.hs_date_entered_procurement,
        UPPER(TRIM(COALESCE(d.deal_currency, 'USD'))) AS deal_currency,
        SAFE_CAST(d.amount AS FLOAT64) AS amount_orig,
        h.dealstage,
        DATE(h.closedate) AS close_date,
        h.hs_manual_forecast_category AS forecast_category
      FROM `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
      LEFT JOIN `tractian-bi.pepeuhacker.hubspot_deals_live` h
        ON CAST(h.dealId AS STRING) = d.deal_id
      WHERE d.pipeline_label = 'Sales MX'
        AND d.account_executive IS NOT NULL
    )
    SELECT
      d.* EXCEPT(amount_orig),
      d.amount_orig,
      -- Cotação vigente NA DATA do deal (a mais recente com valid_date <= a data).
      -- COALESCE em cascata: sem data usável cai na cotação mais antiga; moeda
      -- desconhecida cai em 1.0 e o valor entra como se fosse dólar — melhor que
      -- virar NULL e sumir da soma calado.
      ROUND(d.amount_orig * COALESCE((
        SELECT fx.conversion_usd FROM fx
        WHERE fx.currency = d.deal_currency
          AND fx.valid_date <= COALESCE(d.close_date, d.created_date, CURRENT_DATE())
        ORDER BY fx.valid_date DESC LIMIT 1
      ), (
        SELECT fx.conversion_usd FROM fx
        WHERE fx.currency = d.deal_currency ORDER BY fx.valid_date ASC LIMIT 1
      ), 1.0), 2) AS amount,
      -- >>> IDs de estágio do MX, que NÃO são os do BR. <<<
      -- O funil mexicano é outro pipeline no HubSpot e tem ids próprios:
      -- ganho = 13023548 ("Stage 7 - Closed WON"), perdido = 8352516 ("Lost").
      -- Reusar 'closedwon'/'closedlost' (que são os do BR) fazia NENHUM deal ser
      -- ganho e TODOS parecerem abertos — 1.114 deals abertos e US$ 0 de
      -- realizado, com 486 closed-won na base. Foi assim que o bug apareceu.
      (d.dealstage = '13023548') AS is_won,
      (d.dealstage IS NULL
       OR d.dealstage NOT IN ('13023548','8352516')) AS is_open
    FROM d
    """
    return query(sql, cache_key="mx_deals_raw_v5", force_refresh=force_refresh)


def mx_quota(force_refresh: bool = False) -> pd.DataFrame:
    """Quota trimestral do time MX por rep, em MRR USD.

    Fonte: `sales_rep_monthly_quota` com region='mx' — a mesma tabela que o BR
    usa. A coluna se chama `quota_in_brl` e é BRL de verdade, inclusive no MX
    (a empresa normaliza meta em real), então converte pra dólar aqui: só a
    tela MX consome, e ela é toda em USD."""
    sql = """
    WITH ae AS (
      SELECT DISTINCT CAST(account_executive_id AS STRING) AS id, account_executive
      FROM `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals`
      WHERE account_executive_id IS NOT NULL AND account_executive IS NOT NULL
    ),
    fx AS (
      SELECT conversion_usd FROM `tractian-bi.dbt_analytics_dev.currency_conversion_rates`
      WHERE currency = 'BRL' ORDER BY valid_date DESC LIMIT 1
    )
    SELECT
      DATE_TRUNC(q.month, QUARTER)                        AS quarter,
      ae.account_executive                                AS account_executive,
      SUM(COALESCE(q.quota_in_brl, 0))                    AS quota_brl,
      SUM(COALESCE(q.quota_in_brl, 0)) * (SELECT conversion_usd FROM fx) AS quota_usd_mrr,
      SUM(COALESCE(q.new_logos_quota, 0))                 AS new_logos_quota
    FROM `tractian-bi.tractian_sales.sales_rep_monthly_quota` q
    LEFT JOIN ae ON ae.id = CAST(q.user_id AS STRING)
    WHERE LOWER(q.region) = 'mx'
    GROUP BY quarter, account_executive
    ORDER BY quarter
    """
    return query(sql, cache_key="mx_quota_v1", force_refresh=force_refresh)
