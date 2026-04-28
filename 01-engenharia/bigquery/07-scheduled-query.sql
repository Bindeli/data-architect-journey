-- EXERCÍCIO 7: Scheduled Queries e Automação
-- Objetivo: automatizar cargas periódicas diretamente no BigQuery

-- -------------------------------------------------------
-- Query que será agendada — snapshot diário
-- Substitui os parâmetros @run_date pelo valor da execução agendada
-- -------------------------------------------------------

-- No console GCP: BigQuery > Scheduled Queries > Create Scheduled Query
-- Frequência: diária às 06:00
-- Parâmetro automático disponível: @run_date (data da execução)

INSERT INTO `engenharia_bigquery.taxi_trips_snapshot`
SELECT
  @run_date AS data_carga,
  DATE(trip_start_timestamp) AS dia,
  payment_type,
  COUNT(*) AS total_corridas,
  ROUND(SUM(fare), 2) AS total_faturamento
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE DATE(trip_start_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
GROUP BY 1, 2, 3;


-- -------------------------------------------------------
-- Criar a tabela de destino do snapshot
-- -------------------------------------------------------

CREATE TABLE IF NOT EXISTS engenharia_bigquery.taxi_trips_snapshot (
  data_carga     DATE,
  dia            DATE,
  payment_type   STRING,
  total_corridas INT64,
  total_faturamento FLOAT64
)
PARTITION BY data_carga;


-- -------------------------------------------------------
-- Verificar snapshots carregados
-- -------------------------------------------------------

SELECT
  data_carga,
  COUNT(DISTINCT dia) AS dias_cobertos,
  SUM(total_corridas) AS corridas,
  ROUND(SUM(total_faturamento), 2) AS faturamento
FROM engenharia_bigquery.taxi_trips_snapshot
GROUP BY 1
ORDER BY 1 DESC;
