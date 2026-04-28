-- EXERCÍCIO 6: Custo e Otimização de Queries
-- Objetivo: medir, entender e reduzir custo no BigQuery

-- -------------------------------------------------------
-- INFORMATION_SCHEMA — inspecionar tabelas, partições e jobs
-- -------------------------------------------------------

-- Ver todas as tabelas do dataset com tamanho
SELECT
  table_id,
  ROUND(size_bytes / POW(1024, 3), 2) AS size_gb,
  row_count,
  creation_time,
  last_modified_time
FROM `engenharia_bigquery.__TABLES__`
ORDER BY size_bytes DESC;


-- Ver partições de uma tabela
SELECT
  partition_id,
  total_rows,
  total_logical_bytes,
  last_modified_time
FROM `engenharia_bigquery.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'taxi_trips_otimizada'
ORDER BY partition_id DESC
LIMIT 20;


-- Ver jobs executados (últimas 24h) com bytes processados
SELECT
  job_id,
  user_email,
  creation_time,
  total_bytes_processed,
  ROUND(total_bytes_processed / POW(1024, 4) * 5, 4) AS custo_usd,
  query
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  AND job_type = 'QUERY'
ORDER BY total_bytes_processed DESC
LIMIT 20;


-- -------------------------------------------------------
-- Boas práticas de custo — exemplos comparativos
-- -------------------------------------------------------

-- RUIM: SELECT * (lê todas as colunas)
SELECT *
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE DATE(trip_start_timestamp) = '2023-06-01';

-- BOM: selecionar só as colunas necessárias
SELECT trip_id, fare, payment_type, trip_start_timestamp
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE DATE(trip_start_timestamp) = '2023-06-01';


-- RUIM: filtro que não usa partição
SELECT COUNT(*), SUM(fare)
FROM `engenharia_bigquery.taxi_trips_otimizada`
WHERE EXTRACT(YEAR FROM trip_start_timestamp) = 2023;

-- BOM: filtro que usa a partição diretamente
SELECT COUNT(*), SUM(fare)
FROM `engenharia_bigquery.taxi_trips_otimizada`
WHERE trip_start_timestamp BETWEEN '2023-01-01' AND '2023-12-31';


-- -------------------------------------------------------
-- Estimar custo antes de rodar (dry run via CLI)
-- Execute no terminal:
-- bq query --dry_run --use_legacy_sql=false \
--   'SELECT * FROM `projeto.dataset.tabela` LIMIT 1000'
-- -------------------------------------------------------
