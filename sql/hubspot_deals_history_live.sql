CREATE OR REPLACE VIEW `tractian-bi.pepeuhacker.hubspot_deals_history_live` AS
-- HISTÓRICO VERSIONADO DE ESTÁGIO E AMOUNT POR DEAL, no MESMO shape de
-- `hubspot.hubspot_deals_history` (dealId INT64 + arrays {value, timestamp,
-- updated_by_user_id}), para que todo SQL que já faz `UNNEST(h.dealstage)` siga
-- valendo trocando só o nome da tabela.
--
-- 12/10/2026: `hubspot.hubspot_deals_history` parou em 2026-10-01 23:49 UTC. O
-- PoP Early/Late, o Histórico PoP, o Funil, o Funil WoW, o Stale, o Legal e a foto
-- semanal do Forecast liam dali e passaram a mostrar a semana como parada (pontas
-- iguais à foto de 01/10). A nekt publica o mesmo histórico em formato longo
-- (`analytics_canonical_hubspot_deals_history`: uma linha por mudança de
-- propriedade). Conferido em set/2026 com as duas vivas: 5.897 mudanças de
-- estágio e 2.286 de amount no mesmo deal+timestamp, com o mesmo valor (8
-- diferentes); 75 + 41 só na legada, das quais 17 do Sales BR em um mês.
--
-- Mescla: nekt + legada, deduplicando por (deal, propriedade, timestamp) com
-- preferência pela nekt. A legada só completa o passado que faltar na nekt; quem
-- estiver vivo ganha sozinho, como na hubspot_deals_live.
WITH longo AS (
  SELECT SAFE_CAST(deal_id AS INT64) AS dealId, property_name AS prop,
         valid_from AS ts, property_value AS value,
         SAFE_CAST(updated_by_user_id AS INT64) AS uid, 1 AS prioridade
  FROM `tractian-bi.hubspot_nekt.analytics_canonical_hubspot_deals_history`
  WHERE property_name IN ('dealstage', 'amount')
    AND _sdc_deleted_at IS NULL
    AND valid_from IS NOT NULL
  UNION ALL
  SELECT h.dealId, 'dealstage', s.timestamp, s.value, s.updated_by_user_id, 2
  FROM `tractian-bi.hubspot.hubspot_deals_history` h, UNNEST(h.dealstage) s
  UNION ALL
  SELECT h.dealId, 'amount', a.timestamp, a.value, a.updated_by_user_id, 2
  FROM `tractian-bi.hubspot.hubspot_deals_history` h, UNNEST(h.amount) a
),
unico AS (
  SELECT * FROM longo
  WHERE dealId IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dealId, prop, ts ORDER BY prioridade) = 1
)
SELECT
  dealId,
  ARRAY_AGG(IF(prop = 'dealstage',
                STRUCT(value, ts AS timestamp, uid AS updated_by_user_id), NULL)
            IGNORE NULLS ORDER BY ts) AS dealstage,
  ARRAY_AGG(IF(prop = 'amount',
                STRUCT(value, ts AS timestamp, uid AS updated_by_user_id), NULL)
            IGNORE NULLS ORDER BY ts) AS amount
FROM unico
GROUP BY dealId
