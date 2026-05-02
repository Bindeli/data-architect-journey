-- EXERCÍCIO 1: Particionamento e Clustering no BigQuery
-- Objetivo: entender o impacto de otimização em custo e performance
-- Dataset: bigquery-public-data.thelook_ecommerce (criado pelo Google, acesso garantido)

-- PASSO 1: Crie o dataset 'engenharia_bigquery_us' na região US (multi-region) no GCP

-- -------------------------------------------------------
-- PASSO 2: Crie a tabela otimizada a partir de dados públicos
-- -------------------------------------------------------
CREATE TABLE engenharia_bigquery_us.pedidos_otimizados
PARTITION BY DATE(created_at)
CLUSTER BY status, gender
AS
SELECT *
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE created_at >= '2022-01-01';


-- -------------------------------------------------------
-- PASSO 3: Compare o custo das queries (veja os bytes estimados antes de rodar)
-- -------------------------------------------------------

-- SEM otimização — escaneia a tabela inteira
SELECT
  COUNT(*) AS total_pedidos,
  ROUND(SUM(num_of_item), 2) AS total_itens
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE DATE(created_at) = '2023-06-01';


-- COM otimização — escaneia apenas a partição do dia
SELECT
  COUNT(*) AS total_pedidos,
  ROUND(SUM(num_of_item), 2) AS total_itens
FROM `engenharia_bigquery_us.pedidos_otimizados`
WHERE DATE(created_at) = '2023-06-01';


-- -------------------------------------------------------
-- PASSO 4: Analise o plano de execução
-- -------------------------------------------------------
-- No console do BigQuery, clique em "Execution details" após rodar a query
-- Observe: slot time, bytes processados, estágios do job
