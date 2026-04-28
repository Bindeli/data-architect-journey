-- EXERCÍCIO 5: DML Avançado — MERGE, INSERT, UPDATE, DELETE
-- Objetivo: dominar operações de escrita no BigQuery (essencial para pipelines ELT)

-- -------------------------------------------------------
-- Criação da tabela de destino (staging e destino)
-- -------------------------------------------------------

CREATE TABLE IF NOT EXISTS engenharia_bigquery.taxi_trips_destino
PARTITION BY DATE(trip_start_timestamp)
CLUSTER BY payment_type
AS
SELECT *
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE FALSE;  -- cria estrutura vazia


CREATE TABLE IF NOT EXISTS engenharia_bigquery.taxi_trips_staging
AS
SELECT *
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE DATE(trip_start_timestamp) = '2023-06-01'
LIMIT 1000;


-- -------------------------------------------------------
-- MERGE — upsert (insert se novo, update se existe)
-- Padrão fundamental em pipelines ELT/CDC
-- -------------------------------------------------------

MERGE engenharia_bigquery.taxi_trips_destino AS destino
USING engenharia_bigquery.taxi_trips_staging AS origem
ON destino.trip_id = origem.trip_id

WHEN MATCHED THEN
  UPDATE SET
    fare            = origem.fare,
    payment_type    = origem.payment_type,
    tips            = origem.tips

WHEN NOT MATCHED THEN
  INSERT ROW;


-- -------------------------------------------------------
-- DELETE condicional
-- -------------------------------------------------------

DELETE FROM engenharia_bigquery.taxi_trips_destino
WHERE fare IS NULL OR fare <= 0;


-- -------------------------------------------------------
-- INSERT seletivo
-- -------------------------------------------------------

INSERT INTO engenharia_bigquery.taxi_trips_destino
SELECT *
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE DATE(trip_start_timestamp) = '2023-06-02'
  AND fare > 0
LIMIT 500;


-- -------------------------------------------------------
-- Verificar resultado
-- -------------------------------------------------------

SELECT
  DATE(trip_start_timestamp) AS dia,
  COUNT(*) AS total,
  ROUND(SUM(fare), 2) AS faturamento
FROM engenharia_bigquery.taxi_trips_destino
GROUP BY 1
ORDER BY 1;
