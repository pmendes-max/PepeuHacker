"""BigQuery client with parquet-based cache."""
from __future__ import annotations
import os, re, time, threading, warnings
warnings.filterwarnings("ignore")
# Chave local do service account. SÓ aponta a variável se o arquivo EXISTIR:
# fora do laptop (Cloud Run/VM) a identidade vem do próprio ambiente via ADC, e
# apontar GOOGLE_APPLICATION_CREDENTIALS pra um caminho inexistente faz o ADC
# falhar em vez de cair no fallback — o app subiria e morreria na 1ª query.
_LOCAL_SA = os.path.expanduser("~/.config/gcp/tractian-bi.json")
if os.path.exists(_LOCAL_SA):
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", _LOCAL_SA)

from pathlib import Path
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
        df.to_parquet(cache_file, index=False)
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
}


def canon_ae_names(df: pd.DataFrame) -> pd.DataFrame:
    """Reescreve as variações de AE_NAME_ALIASES no nome canônico."""
    if df is not None and len(df) and "account_executive" in df.columns:
        df["account_executive"] = df["account_executive"].replace(AE_NAME_ALIASES)
    return df


def query(sql: str, cache_key: str | None = None, ttl: int = CACHE_TTL,
          force_refresh: bool = False) -> pd.DataFrame:
    """`_query_raw` + canonização de nome de AE. Wrapper (e não edição dos 6
    pontos de return de `_query_raw`) pra cobrir também os caminhos de cache
    stale e de fallback quando o BQ está fora."""
    return canon_ae_names(_query_raw(sql, cache_key=cache_key, ttl=ttl,
                                     force_refresh=force_refresh))


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
            df.to_parquet(CACHE_DIR / f"{cache_key}.parquet", index=False)
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


def funnel_stage_entries(force_refresh: bool = False) -> pd.DataFrame:
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
    FROM `tractian-bi.hubspot.hubspot_deals_history` h,
         UNNEST(h.dealstage) s
    JOIN `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
      ON CAST(d.deal_id AS INT64) = h.dealId
    WHERE d.pipeline_label = 'Sales BR'
      AND s.value IN ('appointmentscheduled','contractsent','93878633',
                      'decisionmakerboughtin','87188634','65917881',
                      'closedwon','closedlost')
    GROUP BY deal_id, stage
    """
    return query(sql, cache_key="funnel_stage_entries_v5", force_refresh=force_refresh)


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
        -- Outbound = deal_source_agg = 'Outbound' (cobre Referral, outbound, Recovery, Linkedin Networking)
        -- Inbound  = todo o resto (todas marketing-driven + Expansion)
        CASE
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
          CASE WHEN d.deal_type = 'Up & cross sell'                            THEN 'exp'
               WHEN d.deal_type = 'Aquisition' AND d.deal_source_agg='Outbound' THEN 'outbound'
               WHEN d.deal_type = 'Aquisition'                                  THEN 'inbound'
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


def demand_deals_raw(force_refresh: bool = False) -> pd.DataFrame:
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
      d.deal_id,
      d.deal_name,
      COALESCE(SAFE_CAST(d.amount AS FLOAT64), 0) AS amount,
      d.deal_currency,
      d.stage_label,
      d.sdr
    FROM `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
    WHERE d.created_date >= DATE '2024-01-01'
      AND d.created_date <= CURRENT_DATE()
      AND d.account_executive IS NOT NULL
    """
    return query(sql, cache_key="demand_deals_raw_v3", force_refresh=force_refresh)


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
    v AS (
      SELECT
        v.hs_object_id,
        v.start_date,
        CASE WHEN v.hs_pipeline = '890777614' THEN 'ia_center' ELSE 'field' END AS category,
        v.hubspot_owner_id,
        v.collaborators_in_visit
      FROM `tractian-bi.hubspot.hubspot_visits` v
      WHERE v.start_date >= DATE '2024-01-01'
        AND v.start_date <= CURRENT_DATE()
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
    return query(sql, cache_key="visits_raw_v2", force_refresh=force_refresh)


def pipeline_total_raw(force_refresh: bool = False) -> pd.DataFrame:
    """Pipeline ABERTO do Sales BR com os eixos de corte da aba Pipeline Total:
    estágio, deal type, source e forecast category — tudo numa linha por deal,
    com o amount.

    Por que não reusar as queries existentes: `pipeline_deals_raw` tem stage e
    is_open mas não tem forecast category nem source; `forecast_deals_raw` tem
    forecast category mas recorta os estágios 1-6 + won e não traz source. Aqui
    o recorte é "todo o dinheiro que está no funil", então a query é ampla e
    quem filtra é o pandas no app (cache de 1h, mesma estratégia do resto).

    `is_open` vem do RAW do HubSpot (`hubspot.hubspot_deals`), não da curada,
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
      h.hs_manual_forecast_category AS forecast_category,
      d.created_date,
      DATE(h.closedate) AS close_date,
      (h.dealstage IS NULL OR h.dealstage NOT IN ('closedwon','closedlost')) AS is_open
    FROM `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` d
    LEFT JOIN `tractian-bi.hubspot.hubspot_deals` h
      ON CAST(h.dealId AS STRING) = d.deal_id
    WHERE d.pipeline_label = 'Sales BR'
      AND d.account_executive IS NOT NULL
    """
    return query(sql, cache_key="pipeline_total_raw_v1", force_refresh=force_refresh)


def pipeline_deals_raw(force_refresh: bool = False) -> pd.DataFrame:
    """Todos os deals do funil Sales BR. Verdade sobre estado e close date vem
    do RAW HubSpot (`hubspot.hubspot_deals`), não da curated — porque a curated
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
    LEFT JOIN `tractian-bi.hubspot.hubspot_deals` h
      ON CAST(h.dealId AS STRING) = d.deal_id
    WHERE d.pipeline_label = 'Sales BR'
      AND d.account_executive IS NOT NULL
    """
    return query(sql, cache_key="pipeline_deals_raw_v5", force_refresh=force_refresh)


def forecast_deals_raw(force_refresh: bool = False) -> pd.DataFrame:
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
    LEFT JOIN `tractian-bi.hubspot.hubspot_deals` h
      ON CAST(h.dealId AS STRING) = d.deal_id
    WHERE d.pipeline_label = 'Sales BR'
      AND d.account_executive IS NOT NULL
      AND h.dealstage IN ('appointmentscheduled','contractsent','93878633',
                          '87188634','decisionmakerboughtin','65917881','closedwon')
    """
    return query(sql, cache_key="forecast_deals_raw_v5", force_refresh=force_refresh)


def funnel_transitions(t_curr_iso: str, t_prev_iso: str,
                       force_refresh: bool = False) -> pd.DataFrame:
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
      FROM `tractian-bi.hubspot.hubspot_deals_history` h, UNNEST(h.dealstage) s
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
    return query(sql, cache_key=key, ttl=86400, force_refresh=force_refresh)


def wow_events(t_curr_iso: str, t_prev_iso: str,
               force_refresh: bool = False) -> pd.DataFrame:
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
      FROM `tractian-bi.hubspot.hubspot_deals_history` h, UNNEST(h.dealstage) s
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
      FROM `tractian-bi.hubspot.hubspot_deals_history` h, UNNEST(h.amount) a
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
    key = f"wow_events_v1_{t_prev_iso}_{t_curr_iso}".replace(" ", "_").replace(":", "")
    return query(sql, cache_key=key, ttl=86400, force_refresh=force_refresh)


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
                          force_refresh: bool = False) -> pd.DataFrame:
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
      FROM `tractian-bi.hubspot.hubspot_deals_history` h
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
    # Cache key inclui ambos timestamps — invalida quando muda a janela (v2 = +deal_name)
    key = f"pipe_wow_v2_{t_prev_iso}_{t_curr_iso}".replace(" ", "_").replace(":", "")
    return query(sql, cache_key=key, ttl=86400, force_refresh=force_refresh)


def pipeline_stalled(force_refresh: bool = False) -> pd.DataFrame:
    """Deals abertos no funil Sales BR ATUALMENTE em algum dos 5 stages que têm
    SLA de inatividade — devolve estado atual (stage + dias desde a última
    transição). O caller filtra por threshold de cada stage:
      Qualification    (contractsent)          → 14d
      Project Scoping  (93878633)              → 21d
      Negotiation      (decisionmakerboughtin) → 14d
      Consensus        (87188634)              → 14d
      Procurement      (65917881)              → 21d
    Reconstrução via último item do array `dealstage` em `hubspot_deals_history`."""
    sql = """
    WITH cur AS (
      SELECT
        h.dealId,
        (SELECT s.value FROM UNNEST(h.dealstage) s
           WHERE s.timestamp <= CURRENT_TIMESTAMP()
           ORDER BY s.timestamp DESC LIMIT 1) AS stage_curr,
        (SELECT s.timestamp FROM UNNEST(h.dealstage) s
           WHERE s.timestamp <= CURRENT_TIMESTAMP()
           ORDER BY s.timestamp DESC LIMIT 1) AS stage_changed_at
      FROM `tractian-bi.hubspot.hubspot_deals_history` h
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
    return query(sql, cache_key="pipeline_stalled_v2", ttl=600, force_refresh=force_refresh)


def pipeline_stalled_wow(t_curr_iso: str, t_prev_iso: str,
                         force_refresh: bool = False) -> pd.DataFrame:
    """Estado de cada deal nos 2 snapshots (t_curr, t_prev) — usado pra calcular
    quem entrou/saiu da inatividade WoW. Universo: deals do funil Sales BR
    que tocam algum dos 5 stages com SLA em pelo menos um dos snapshots.

    Retorna por deal:
      stage_curr, changed_at_curr, days_curr
      stage_prev, changed_at_prev, days_prev"""
    sql = f"""
    WITH snaps AS (
      SELECT
        h.dealId,
        (SELECT s.value FROM UNNEST(h.dealstage) s
           WHERE s.timestamp <= TIMESTAMP('{t_curr_iso}')
           ORDER BY s.timestamp DESC LIMIT 1) AS stage_curr,
        (SELECT s.timestamp FROM UNNEST(h.dealstage) s
           WHERE s.timestamp <= TIMESTAMP('{t_curr_iso}')
           ORDER BY s.timestamp DESC LIMIT 1) AS changed_at_curr,
        (SELECT s.value FROM UNNEST(h.dealstage) s
           WHERE s.timestamp <= TIMESTAMP('{t_prev_iso}')
           ORDER BY s.timestamp DESC LIMIT 1) AS stage_prev,
        (SELECT s.timestamp FROM UNNEST(h.dealstage) s
           WHERE s.timestamp <= TIMESTAMP('{t_prev_iso}')
           ORDER BY s.timestamp DESC LIMIT 1) AS changed_at_prev
      FROM `tractian-bi.hubspot.hubspot_deals_history` h
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
    key = f"pipe_stalled_wow2_{t_prev_iso}_{t_curr_iso}".replace(" ", "_").replace(":", "")
    return query(sql, cache_key=key, ttl=86400, force_refresh=force_refresh)


# ---------------------------------------------------------------------------
# Inatividade por ATIVIDADE (V2) — mede dias desde o último RETORNO de um contato
# do deal, não dias no stage. "Retorno" = engagement de duas vias, atrelado ao
# deal via a ponte contato↔deal (hubspot_vid_associated_with_deal_id):
#   • email  : INCOMING_EMAIL (resposta do contato), exceto out-of-office
#   • call   : INBOUND (contato ligou) OU outbound conectada (duração ≥ 30s)
# Meetings NÃO entram: a tabela hubspot_meetings no BQ não traz associação
# (sem contato/deal/participante), então precisaria da API de associações.
# `_ACTIVITY_LOOKBACK_DAYS` limita o scan das tabelas gigantes de engagement:
# atividade mais velha que isso já estoura qualquer SLA, então cai no fallback
# (stage_changed_at) sem custo de varrer o histórico inteiro.
_ACTIVITY_LOOKBACK_DAYS = 365

_STALLED_STAGE_IDS_SQL = (
    "'contractsent','93878633','decisionmakerboughtin','87188634','65917881'")


def _activity_cte(asof_iso: str | None, sfx: str = "") -> tuple[str, str]:
    """Constrói as CTEs do último retorno de contato por deal e devolve
    (sql_das_ctes, nome_da_cte_final). `sfx` sufixa os nomes das CTEs pra poder
    montar dois snapshots (curr/prev) no mesmo WITH sem colisão. asof_iso=None →
    estado atual [now-lookback, now]; senão, snapshot histórico [asof-lookback, asof]."""
    hi = f"TIMESTAMP('{asof_iso}')" if asof_iso else "CURRENT_TIMESTAMP()"
    lo = f"TIMESTAMP_SUB({hi}, INTERVAL {_ACTIVITY_LOOKBACK_DAYS} DAY)"
    br, em, ca, ac, la = (f"bridge{sfx}", f"email_ret{sfx}", f"call_ret{sfx}",
                          f"acts{sfx}", f"last_act{sfx}")
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
    {ac} AS (
      SELECT * FROM {em} UNION ALL SELECT * FROM {ca}
    ),
    {la} AS (
      SELECT deal_id,
        MAX(ts)                          AS last_activity_at,
        MAX(IF(ch = 'email', ts, NULL))  AS last_email_at,
        MAX(IF(ch = 'call',  ts, NULL))  AS last_call_at
      FROM {ac} WHERE ts IS NOT NULL GROUP BY deal_id
    )"""
    return cte, la


def pipeline_activity(force_refresh: bool = False) -> pd.DataFrame:
    """Deals abertos no funil Sales BR ATUALMENTE nos 5 stages com SLA — devolve,
    por deal, o stage atual e a data do último RETORNO de um contato (email
    entrante / call conectada). `days_since_activity` = dias desde esse retorno;
    quando nunca houve retorno (na janela), cai no `stage_changed_at` como origem
    justa (não pune deal recém-chegado ao stage). `has_activity` diferencia os
    dois casos. Espelha pipeline_stalled(), trocando o eixo de tempo."""
    act_cte, la = _activity_cte(None)
    sql = f"""
    WITH cur AS (
      SELECT
        h.dealId,
        (SELECT s.value FROM UNNEST(h.dealstage) s
           WHERE s.timestamp <= CURRENT_TIMESTAMP()
           ORDER BY s.timestamp DESC LIMIT 1) AS stage_curr,
        (SELECT s.timestamp FROM UNNEST(h.dealstage) s
           WHERE s.timestamp <= CURRENT_TIMESTAMP()
           ORDER BY s.timestamp DESC LIMIT 1) AS stage_changed_at
      FROM `tractian-bi.hubspot.hubspot_deals_history` h
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
      AND cur.stage_curr IN ({_STALLED_STAGE_IDS_SQL})
      AND cur.stage_changed_at IS NOT NULL
    """
    return query(sql, cache_key="pipeline_activity_v1", ttl=600, force_refresh=force_refresh)


def pipeline_activity_wow(t_curr_iso: str, t_prev_iso: str,
                          force_refresh: bool = False) -> pd.DataFrame:
    """Estado de atividade de cada deal em 2 snapshots (domingos), pra calcular
    quem saiu/entrou da inatividade-por-atividade WoW. Por snapshot: stage as-of
    (do histórico) + último retorno de contato as-of + days_since as-of. Mesmo
    universo/padrão de pipeline_stalled_wow()."""
    def _snap(hi_iso, suffix):
        return f"""
        stage_{suffix} AS (
          SELECT
            h.dealId,
            (SELECT s.value FROM UNNEST(h.dealstage) s
               WHERE s.timestamp <= TIMESTAMP('{hi_iso}')
               ORDER BY s.timestamp DESC LIMIT 1) AS stage,
            (SELECT s.timestamp FROM UNNEST(h.dealstage) s
               WHERE s.timestamp <= TIMESTAMP('{hi_iso}')
               ORDER BY s.timestamp DESC LIMIT 1) AS changed_at
          FROM `tractian-bi.hubspot.hubspot_deals_history` h
        )"""
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
      AND (sc.stage IN ({_STALLED_STAGE_IDS_SQL})
        OR sp.stage IN ({_STALLED_STAGE_IDS_SQL}))
    """
    # v2 = +deal_name/amount (drill da movimentação WoW da página Stale · Atividade)
    key = f"pipe_activity_wow2_{t_prev_iso}_{t_curr_iso}".replace(" ", "_").replace(":", "")
    return query(sql, cache_key=key, ttl=86400, force_refresh=force_refresh)


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
