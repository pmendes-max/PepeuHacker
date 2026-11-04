CREATE OR REPLACE VIEW `tractian-bi.pepeuhacker.hubspot_deals_stage_live` AS
-- ESTÁGIO AO VIVO + DATA DE ENTRADA, por deal — base da Inatividade e do Stale.
--
-- 08/10/2026: `hubspot.hubspot_deals_history` (array dealstage versionado) parou
-- em 2026-10-01 23:49 UTC. As telas de inatividade liam o estágio atual de lá e
-- passaram a mostrar deal já ganho/perdido como parado e estágio velho com o SLA
-- errado (8 de 30 deals numa conferência contra o HubSpot). O HubSpot guarda a
-- data de entrada em cada estágio como propriedade do deal (hs_v2_date_entered_*),
-- e as duas tabelas vivas trazem essas colunas — então o estado atual sai daqui,
-- sem depender do histórico.
--
-- Mesma mescla da hubspot_deals_live: legada + nekt transformed, fica a linha mais
-- recente de cada deal por hs_lastmodifieddate (quem estiver vivo ganha sozinho).
-- `stage_entered_at` = hs_v2_date_entered_current_stage (DATE na nekt, TIMESTAMP
-- na legada; aqui sempre TIMESTAMP). As colunas por estágio servem pros
-- snapshots de domingo do WoW (BR e MX; ids do MX em bq.STAGE_ID_MAP).
WITH uniao AS (
  SELECT
    CAST(dealId AS STRING) AS dealId, dealstage, createdate, hs_lastmodifieddate,
    CAST(hs_v2_date_entered_current_stage AS TIMESTAMP) AS stage_entered_at,
    hs_v2_date_entered_appointmentscheduled, hs_v2_date_entered_8352441,
    hs_v2_date_entered_contractsent, hs_v2_date_entered_93878633,
    hs_v2_date_entered_decisionmakerboughtin, hs_v2_date_entered_87188634,
    hs_v2_date_entered_65917881,
    hs_v2_date_entered_8352512, hs_v2_date_entered_94159788, hs_v2_date_entered_8352513,
    hs_v2_date_entered_89673855, hs_v2_date_entered_66011247,
    2 AS prioridade_empate
  FROM `tractian-bi.hubspot.hubspot_deals`
  UNION ALL
  SELECT
    CAST(CAST(dealid AS INT64) AS STRING), dealstage, createdate, hs_lastmodifieddate,
    CAST(hs_v2_date_entered_current_stage AS TIMESTAMP),
    hs_v2_date_entered_appointmentscheduled, hs_v2_date_entered_8352441,
    hs_v2_date_entered_contractsent, hs_v2_date_entered_93878633,
    hs_v2_date_entered_decisionmakerboughtin, hs_v2_date_entered_87188634,
    hs_v2_date_entered_65917881,
    hs_v2_date_entered_8352512, hs_v2_date_entered_94159788, hs_v2_date_entered_8352513,
    hs_v2_date_entered_89673855, hs_v2_date_entered_66011247,
    1
  FROM `tractian-bi.hubspot_nekt.hubspot_deals_transformed`
  WHERE _sdc_deleted_at IS NULL
)
SELECT * EXCEPT (prioridade_empate)
FROM uniao
WHERE dealId IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY dealId
  ORDER BY hs_lastmodifieddate DESC NULLS LAST, prioridade_empate DESC
) = 1
