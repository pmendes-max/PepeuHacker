CREATE OR REPLACE VIEW `tractian-bi.pepeuhacker.hubspot_deals_live` AS
-- ESTADO AO VIVO DOS DEALS DO HUBSPOT — dealId, dealstage, closedate,
-- hs_manual_forecast_category (+ hs_lastmodifieddate, que alimenta o selo de
-- frescor das telas). Shape da tabela legada, para que todo SQL que já diz
-- `h.dealstage` siga valendo.
--
-- 15/09/2026: a view deixou de apontar para UMA tabela e passou a MESCLAR as
-- duas fontes vivas, ficando com a linha mais recente de cada deal. Motivo: as
-- duas já trocaram de papel duas vezes em cinco dias e cada troca serviu dado
-- velho com cara de novo.
--   11/09  `hubspot.hubspot_deals` (legada) parou de receber sync
--          -> a view passou a ler a nekt crua.
--   14/09  a nekt crua `hubspot_nekt.hubspot_deals` congelou às 10h56, quando o
--          dbt do BI migrou para `hubspot_deals_transformed` com outro nome
--          -> o dashboard passou a servir o funil de domingo, e a legada, que
--             voltou a receber sync completo (36 mil deals às 01h de 15/09),
--             já estava 27h à frente dela.
-- Mesclando, quem estiver vivo ganha sozinho: não há troca manual a fazer na
-- próxima vez que uma das duas parar.
--
-- O desempate é por `hs_lastmodifieddate` (campo do próprio HubSpot, não
-- metadado de gravação do BigQuery — a legada recebe INSERTs avulsos de
-- aplicações que deixam o `last_modified_time` da tabela com cara de recente
-- mesmo quando o sync está morto). Empate desempata pela legada, que hoje é a
-- que tem o sync completo.
WITH uniao AS (
  SELECT
    CAST(dealId AS STRING)      AS dealId,
    dealstage                   AS dealstage,
    -- Epoch = "sem close date" (ver nota da nekt abaixo). Normalizado nas duas
    -- fontes pra a mescla nunca depender de quem ganhou o desempate.
    -- Data em BRASÍLIA, sem hora (ver nota "CLOSE DATE É DATA" no fim).
    IF(DATE(closedate) = '1970-01-01', NULL,
       TIMESTAMP(DATE(closedate, 'America/Sao_Paulo'))) AS closedate,
    hs_manual_forecast_category AS hs_manual_forecast_category,
    hs_lastmodifieddate         AS hs_lastmodifieddate,
    2                           AS prioridade_empate
  FROM `tractian-bi.hubspot.hubspot_deals`
  UNION ALL
  SELECT
    CAST(dealid AS STRING),
    dealstage,
    -- 12/10/2026: a nekt passou a gravar close date VAZIO como 1970-01-01 em
    -- vez de NULL (5.372 deals, zero NULL). Deal sem data virava deal "fechando
    -- em 1970": o filtro "Sem close date" do Pipeline Total ficava vazio e esses
    -- deals contavam como vencidos onde há corte por data.
    IF(DATE(closedate) = '1970-01-01', NULL,
       TIMESTAMP(DATE(closedate, 'America/Sao_Paulo'))),
    hs_manual_forecast_category,
    hs_lastmodifieddate,
    1
  FROM `tractian-bi.hubspot_nekt.hubspot_deals_transformed`
  -- 01/10/2026: a tabela passou a ser recriada pelo `nekt-connector` (antes era
  -- o `gchagas-dbt`) e perdeu a coluna `archived` — a view parou de compilar e
  -- derrubou o dashboard. O marcador de exclusão agora é o do padrão Singer.
  WHERE _sdc_deleted_at IS NULL
  UNION ALL
  -- 13/10/2026: a nekt e a legada pararam em 11/10 20h01 BRT (a nekt seguia
  -- extraindo, mas sem nenhuma alteração nova). Esta tabela é carregada pelo
  -- conector do HubSpot do Claude (`tools/deals_ingest.py`) com o estado ATUAL
  -- dos deals abertos dos funis Sales BR e Sales MX e dos que fecharam desde a
  -- parada. Entra no mesmo desempate por `hs_lastmodifieddate`: quando a nekt
  -- voltar com versão mais nova, ela ganha sozinha.
  SELECT
    dealId,
    dealstage,
    IF(DATE(closedate) = '1970-01-01', NULL,
       TIMESTAMP(DATE(closedate, 'America/Sao_Paulo'))),
    hs_manual_forecast_category,
    hs_lastmodifieddate,
    3
  FROM `tractian-bi.pepeuhacker.hubspot_deals_mcp`
)
-- CLOSE DATE É DATA (13/10/2026, pedido do Pedro). O HubSpot grava o close
-- date com a HORA em que o AE escolheu a data, e todo SQL do app fazia
-- DATE(h.closedate) — em UTC. Quem marcava 31/12/2026 às 21h ou depois
-- (horário de Brasília) caía em 01/01/2027 e saía do Q4: 11 deals e R$ 314 mil
-- em 13/10, entre eles Sinobras (R$ 120 mil). O desvio valia o ano todo: 363
-- deals do Sales BR e 679 do Sales MX com o close date um dia à frente da
-- curated do BI. A view devolve a data de Brasília à meia-noite, então
-- `DATE(h.closedate)` em qualquer consulta é a data que o HubSpot mostra — e
-- bate 100% com `int_adding_names_to_deals.close_date` (o BI também usa
-- Brasília, inclusive pro México e EUA). A hora do close date não é usada em
-- lugar nenhum do app.
SELECT dealId, dealstage, closedate, hs_manual_forecast_category, hs_lastmodifieddate
FROM uniao
WHERE dealId IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY dealId
  ORDER BY hs_lastmodifieddate DESC NULLS LAST, prioridade_empate DESC
) = 1
