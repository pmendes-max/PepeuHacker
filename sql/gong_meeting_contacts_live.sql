CREATE OR REPLACE VIEW `tractian-bi.pepeuhacker.gong_meeting_contacts_live` AS
-- REUNIÃO DO GONG POR CONTATO — canal 'gong' da Inatividade (bq._activity_cte).
--
-- 19/10/2026: o Pedro pediu que reunião feita no Gong conte como retorno do
-- cliente quando o CONTATO da reunião está associado ao deal do pipeline de
-- vendas. O Gong grava toda reunião gravada no HubSpot como meeting
-- ("[Gong] Microsoft Teams: ...", hs_object_source_detail_1 = 'Gong',
-- outcome COMPLETED) — a Inatividade só não lia meeting nenhum.
--
-- Duas fontes, mesma regra (participante = contato do HubSpot):
--   • nekt (`hubspot_nekt.hubspot_meetings`): até 29/09/2026 17h UTC, quando a
--     cópia parou junto com e-mails e ligações. Participante pelo e-mail
--     (hs_all_attendee_emails) → contato com esse e-mail.
--   • conector (`pepeuhacker.gong_meeting_contacts`, tools/gong_ingest.py):
--     de 29/09 em diante, contatos ASSOCIADOS às reuniões do dia, no grão de
--     DIA UTC (ts = meia-noite) — a busca devolve os contatos de um lote de
--     reuniões sem dizer qual é de qual.
-- Sobreposição entre as duas não importa: a Inatividade fica com o MAX(ts).
-- Funcionário da Tractian cadastrado como contato não é cliente e sai.
-- A ligação contato → deal (e o "deal do pipeline de vendas") fica no
-- _activity_cte, pela mesma ponte contato↔deal dos e-mails e ligações.
WITH contatos AS (
  SELECT CAST(CAST(vid AS INT64) AS STRING) AS contact_id, LOWER(TRIM(email)) AS email
  FROM `tractian-bi.hubspot_nekt.hubspot_contacts_transformed`
),
nekt AS (
  SELECT SAFE_CAST(m.properties.hs_meeting_start_time AS TIMESTAMP) AS ts,
         LOWER(TRIM(e)) AS email
  FROM `tractian-bi.hubspot_nekt.hubspot_meetings` m,
       UNNEST(SPLIT(m.properties.hs_all_attendee_emails, ';')) e
  WHERE m.properties.hs_object_source_detail_1 = 'Gong'
    AND m._sdc_deleted_at IS NULL
    AND COALESCE(m.properties.hs_meeting_outcome, 'COMPLETED') = 'COMPLETED'
)
SELECT c.contact_id, n.ts, 'nekt' AS fonte
FROM nekt n
JOIN contatos c ON c.email = n.email
WHERE n.ts IS NOT NULL AND n.email != '' AND NOT ENDS_WITH(n.email, '@tractian.com')
UNION ALL
SELECT g.contact_id, TIMESTAMP(g.meeting_date) AS ts, 'conector' AS fonte
FROM `tractian-bi.pepeuhacker.gong_meeting_contacts` g
LEFT JOIN contatos c USING (contact_id)
WHERE NOT ENDS_WITH(IFNULL(c.email, ''), '@tractian.com')
