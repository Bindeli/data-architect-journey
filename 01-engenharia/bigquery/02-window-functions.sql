-- EXERCÍCIO 2: Window Functions no BigQuery
-- Objetivo: dominar funções analíticas para análises avançadas sem GROUP BY

-- -------------------------------------------------------
-- RANK, DENSE_RANK, ROW_NUMBER
-- -------------------------------------------------------

-- Top 3 motoristas com maior faturamento por mês
SELECT
  DATE_TRUNC(trip_start_timestamp, MONTH) AS mes,
  taxi_id,
  ROUND(SUM(fare), 2) AS total_faturamento,
  RANK() OVER (
    PARTITION BY DATE_TRUNC(trip_start_timestamp, MONTH)
    ORDER BY SUM(fare) DESC
  ) AS ranking
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE trip_start_timestamp >= '2023-01-01'
  AND fare IS NOT NULL
GROUP BY 1, 2
QUALIFY ranking <= 3;


-- -------------------------------------------------------
-- LAG / LEAD — comparar com período anterior
-- -------------------------------------------------------

-- Variação de faturamento diário em relação ao dia anterior
WITH faturamento_diario AS (
  SELECT
    DATE(trip_start_timestamp) AS dia,
    ROUND(SUM(fare), 2) AS total
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  WHERE trip_start_timestamp BETWEEN '2023-01-01' AND '2023-03-31'
  GROUP BY 1
)
SELECT
  dia,
  total,
  LAG(total) OVER (ORDER BY dia) AS total_dia_anterior,
  ROUND(total - LAG(total) OVER (ORDER BY dia), 2) AS variacao,
  ROUND(
    SAFE_DIVIDE(total - LAG(total) OVER (ORDER BY dia), LAG(total) OVER (ORDER BY dia)) * 100, 2
  ) AS variacao_pct
FROM faturamento_diario
ORDER BY dia;


-- -------------------------------------------------------
-- RUNNING TOTAL — acumulado
-- -------------------------------------------------------

WITH faturamento_diario AS (
  SELECT
    DATE(trip_start_timestamp) AS dia,
    ROUND(SUM(fare), 2) AS total
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  WHERE trip_start_timestamp BETWEEN '2023-01-01' AND '2023-01-31'
  GROUP BY 1
)
SELECT
  dia,
  total,
  ROUND(SUM(total) OVER (ORDER BY dia ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS acumulado
FROM faturamento_diario
ORDER BY dia;


-- -------------------------------------------------------
-- PERCENTIL e distribuição
-- -------------------------------------------------------

SELECT
  payment_type,
  ROUND(AVG(fare), 2) AS media,
  ROUND(PERCENTILE_CONT(fare, 0.5) OVER (PARTITION BY payment_type), 2) AS mediana,
  ROUND(PERCENTILE_CONT(fare, 0.95) OVER (PARTITION BY payment_type), 2) AS p95
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE fare IS NOT NULL AND fare > 0
LIMIT 1000;
