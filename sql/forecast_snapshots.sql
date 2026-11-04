-- Foto SEMANAL do forecast por deal — base da série ponderada do Histórico do
-- Forecast e validação independente da cópia diária do BI (bate na 3ª casa).
-- Gravada pelo próprio app (refresher, best-effort); a service account precisa
-- de dataEditor no dataset pepeuhacker. Schema conferido contra a tabela real
-- em 10/10/2026.
CREATE TABLE IF NOT EXISTS `tractian-bi.pepeuhacker.forecast_snapshots` (
  snapshot_date     DATE,
  captured_at       TIMESTAMP,
  deal_id           STRING,
  deal_name         STRING,
  account_executive STRING,
  stage             STRING,
  amount            FLOAT64,
  close_date        DATE,
  forecast_category STRING
)
PARTITION BY snapshot_date
CLUSTER BY account_executive;
