CREATE TABLE IF NOT EXISTS `tractian-bi.pepeuhacker.forecast_snapshots_daily` (
  -- O DIA cuja MEIA-NOITE (00:00 BRT) esta foto representa. A foto de
  -- 24/09/2026 é o estado do funil na virada de 23 pra 24 — o fim do dia 23.
  snapshot_date     DATE      NOT NULL,
  -- Instante real da gravação. A captura roda no 1º minuto depois da virada
  -- (ver `_snapshot_forecast_midnight` no app); o campo existe pra defasagem
  -- ficar medível em vez de suposta.
  captured_at       TIMESTAMP NOT NULL,
  -- MAX(hs_lastmodifieddate) da view viva no momento da captura: diz de QUANDO
  -- é o dado que a foto congelou (a fonte pode estar horas atrás do relógio).
  source_at         TIMESTAMP,
  deal_id           STRING    NOT NULL,
  deal_name         STRING,
  account_executive STRING,
  deal_type         STRING,
  stage             STRING,
  amount            FLOAT64,
  close_date        DATE,
  forecast_category STRING,
  po_emission_date  DATE,
  -- Funil da foto: 'Sales BR' ou 'Sales MX'. Entrou em 12/10/2026, quando o app
  -- passou a fotografar o México também; as linhas anteriores foram marcadas
  -- 'Sales BR' (até então só o Brasil era gravado). Aplicado com
  -- ALTER TABLE ... ADD COLUMN pipeline STRING + UPDATE.
  -- `stage` fica no id CRU do funil (o app normaliza na leitura) e `amount` na
  -- moeda do deal (o app converte pra USD na leitura, como em toda query MX).
  pipeline          STRING
)
PARTITION BY snapshot_date
OPTIONS (description =
  'Foto do forecast (Sales BR e Sales MX) à meia-noite BRT de cada dia (DoD do pepeuhacker). '
  'Estado (estágio, close date, forecast category) da view hubspot_deals_live; '
  'amount/AE/deal type/PO da curated int_adding_names_to_deals. Gravada pelo app.');
