-- EXERCÍCIO 2: Window Functions no BigQuery
-- Objetivo: dominar funções analíticas para análises avançadas sem GROUP BY
-- Dataset: bigquery-public-data.thelook_ecommerce

-- -------------------------------------------------------
-- RANK, DENSE_RANK, ROW_NUMBER
-- -------------------------------------------------------

-- Top 3 usuários com mais pedidos por mês
-- Separa agregação e window function em CTEs — BigQuery não permite referenciar
-- colunas não-agregadas diretamente no PARTITION BY após um GROUP BY
WITH pedidos_por_mes AS (
  SELECT
    DATE_TRUNC(created_at, MONTH) AS mes,
    user_id,
    COUNT(*) AS total_pedidos
  FROM `bigquery-public-data.thelook_ecommerce.orders`
  WHERE created_at >= '2023-01-01'
  GROUP BY 1, 2
)
SELECT
  mes,
  user_id,
  total_pedidos,
  RANK() OVER (
    PARTITION BY mes
    ORDER BY total_pedidos DESC
  ) AS ranking
FROM pedidos_por_mes
QUALIFY ranking <= 3;


-- -------------------------------------------------------
-- LAG / LEAD — comparar com período anterior
-- -------------------------------------------------------

-- Variação de pedidos diários em relação ao dia anterior
WITH pedidos_diarios AS (
  SELECT
    DATE(created_at) AS dia,
    COUNT(*) AS total
  FROM `bigquery-public-data.thelook_ecommerce.orders`
  WHERE created_at BETWEEN '2023-01-01' AND '2023-03-31'
  GROUP BY 1
)
SELECT
  dia,
  total,
  LAG(total) OVER (ORDER BY dia) AS total_dia_anterior,
  total - LAG(total) OVER (ORDER BY dia) AS variacao,
  ROUND(
    SAFE_DIVIDE(total - LAG(total) OVER (ORDER BY dia), LAG(total) OVER (ORDER BY dia)) * 100, 2
  ) AS variacao_pct
FROM pedidos_diarios
ORDER BY dia;


-- -------------------------------------------------------
-- RUNNING TOTAL — acumulado
-- -------------------------------------------------------

WITH pedidos_diarios AS (
  SELECT
    DATE(created_at) AS dia,
    COUNT(*) AS total
  FROM `bigquery-public-data.thelook_ecommerce.orders`
  WHERE created_at BETWEEN '2023-01-01' AND '2023-01-31'
  GROUP BY 1
)
SELECT
  dia,
  total,
  SUM(total) OVER (ORDER BY dia ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acumulado
FROM pedidos_diarios
ORDER BY dia;


-- -------------------------------------------------------
-- PERCENTIL e distribuição
-- -------------------------------------------------------

SELECT
  status,
  COUNT(*) AS total,
  ROUND(PERCENTILE_CONT(num_of_item, 0.5) OVER (PARTITION BY status), 2) AS mediana_itens,
  ROUND(PERCENTILE_CONT(num_of_item, 0.95) OVER (PARTITION BY status), 2) AS p95_itens
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE num_of_item IS NOT NULL
LIMIT 1000;
