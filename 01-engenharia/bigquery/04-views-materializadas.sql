-- EXERCÍCIO 4: Views e Views Materializadas
-- Objetivo: entender quando usar cada tipo e o impacto em custo/performance

-- -------------------------------------------------------
-- VIEW COMUM — lógica reutilizável, sem custo de armazenamento
-- Executa a query toda vez que for chamada
-- -------------------------------------------------------

CREATE OR REPLACE VIEW engenharia_bigquery.vw_faturamento_diario AS
SELECT
  DATE(trip_start_timestamp) AS dia,
  COUNT(*) AS total_corridas,
  ROUND(SUM(fare), 2) AS total_faturamento,
  ROUND(AVG(fare), 2) AS ticket_medio
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE fare > 0
GROUP BY 1;


-- -------------------------------------------------------
-- VIEW MATERIALIZADA — resultado pré-computado e atualizado automaticamente
-- Custo de armazenamento, mas queries muito mais rápidas e baratas
-- -------------------------------------------------------

CREATE MATERIALIZED VIEW engenharia_bigquery.mv_faturamento_mensal
OPTIONS (enable_refresh = true, refresh_interval_minutes = 60)
AS
SELECT
  DATE_TRUNC(trip_start_timestamp, MONTH) AS mes,
  payment_type,
  COUNT(*) AS total_corridas,
  ROUND(SUM(fare), 2) AS total_faturamento
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE fare > 0
GROUP BY 1, 2;


-- -------------------------------------------------------
-- Comparar: query direto vs. query na view materializada
-- -------------------------------------------------------

-- Direto na tabela (escaneia tudo)
SELECT mes, SUM(total_faturamento) AS faturamento
FROM (
  SELECT
    DATE_TRUNC(trip_start_timestamp, MONTH) AS mes,
    fare AS total_faturamento
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  WHERE fare > 0
)
GROUP BY 1
ORDER BY 1;

-- Via view materializada (lê resultado pré-computado)
SELECT mes, SUM(total_faturamento) AS faturamento
FROM engenharia_bigquery.mv_faturamento_mensal
GROUP BY 1
ORDER BY 1;


-- -------------------------------------------------------
-- Quando usar cada uma?
-- VIEW COMUM      → lógica de negócio, dados sempre frescos, baixo volume
-- VIEW MATER.     → agregações frequentes, alto volume, tolera dados com delay
-- -------------------------------------------------------
