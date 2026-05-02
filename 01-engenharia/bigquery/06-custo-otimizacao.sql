-- EXERCÍCIO 6: Custo e Otimização de Queries
-- Objetivo: medir, entender e reduzir custo no BigQuery
-- Dataset: bigquery-public-data.thelook_ecommerce + engenharia_bigquery

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
WHERE table_name = 'pedidos_otimizados'
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
FROM `region-us-central1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  AND job_type = 'QUERY'
ORDER BY total_bytes_processed DESC
LIMIT 20;


-- -------------------------------------------------------
-- Boas práticas de custo — exemplos comparativos
-- -------------------------------------------------------

-- RUIM: SELECT * (lê todas as colunas)
SELECT *
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE DATE(created_at) = '2023-06-01';

-- BOM: selecionar só as colunas necessárias
SELECT order_id, user_id, status, num_of_item, created_at
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE DATE(created_at) = '2023-06-01';


-- RUIM: filtro que não usa partição (função sobre a coluna impede pruning)
SELECT COUNT(*), SUM(num_of_item)
FROM `engenharia_bigquery.pedidos_otimizados`
WHERE EXTRACT(YEAR FROM created_at) = 2023;

-- BOM: filtro que usa a partição diretamente
SELECT COUNT(*), SUM(num_of_item)
FROM `engenharia_bigquery.pedidos_otimizados`
WHERE created_at BETWEEN '2023-01-01' AND '2023-12-31';


-- -------------------------------------------------------
-- Estimar custo antes de rodar (dry run via CLI)
-- Execute no terminal:
-- bq query --dry_run --use_legacy_sql=false \
--   'SELECT * FROM `projeto.dataset.tabela` LIMIT 1000'
-- -------------------------------------------------------
