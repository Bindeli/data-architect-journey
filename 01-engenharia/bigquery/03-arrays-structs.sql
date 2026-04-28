-- EXERCÍCIO 3: Arrays e Structs no BigQuery
-- Objetivo: trabalhar com tipos complexos — essencial para dados aninhados (JSON, Avro, Parquet)

-- -------------------------------------------------------
-- STRUCT — agrupar campos relacionados
-- -------------------------------------------------------

SELECT
  trip_id,
  STRUCT(
    pickup_latitude  AS lat,
    pickup_longitude AS lng,
    pickup_community_area AS area
  ) AS origem,
  STRUCT(
    dropoff_latitude  AS lat,
    dropoff_longitude AS lng,
    dropoff_community_area AS area
  ) AS destino,
  fare
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE pickup_latitude IS NOT NULL
LIMIT 10;


-- -------------------------------------------------------
-- ARRAY_AGG — agregar valores em array
-- -------------------------------------------------------

-- Todos os tipos de pagamento usados por cada motorista
SELECT
  taxi_id,
  ARRAY_AGG(DISTINCT payment_type IGNORE NULLS ORDER BY payment_type) AS formas_pagamento,
  COUNT(*) AS total_corridas
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE taxi_id IS NOT NULL
GROUP BY taxi_id
ORDER BY total_corridas DESC
LIMIT 20;


-- -------------------------------------------------------
-- UNNEST — explodir array em linhas
-- -------------------------------------------------------

WITH motoristas AS (
  SELECT
    taxi_id,
    ARRAY_AGG(DISTINCT payment_type IGNORE NULLS) AS formas_pagamento
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  WHERE taxi_id IS NOT NULL
  GROUP BY taxi_id
  LIMIT 5
)
SELECT
  taxi_id,
  forma
FROM motoristas,
UNNEST(formas_pagamento) AS forma;


-- -------------------------------------------------------
-- JSON_EXTRACT — trabalhar com colunas JSON (string)
-- -------------------------------------------------------

-- Simulando uma coluna JSON
WITH dados_json AS (
  SELECT '{"motorista": "ABC123", "avaliacao": 4.8, "tags": ["rapido", "educado"]}' AS payload
)
SELECT
  JSON_EXTRACT_SCALAR(payload, '$.motorista') AS motorista,
  CAST(JSON_EXTRACT_SCALAR(payload, '$.avaliacao') AS FLOAT64) AS avaliacao,
  JSON_EXTRACT_ARRAY(payload, '$.tags') AS tags
FROM dados_json;
