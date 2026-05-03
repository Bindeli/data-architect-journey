-- EXERCÍCIO 2: Window Functions no BigQuery
-- Objetivo: dominar funções analíticas para análises avançadas sem GROUP BY
-- Dataset: bigquery-public-data.thelook_ecommerce

-- -------------------------------------------------------
-- QUERY 1 — RANK: Top 3 usuários por mês
--
-- O que faz: Encontra os 3 usuários que mais fizeram pedidos em cada mês.
--
-- Como funciona:
--   Começa agregando os pedidos por mês e usuário no CTE pedidos_por_mes.
--   Aplica RANK() particionando por mês — cada mês tem seu próprio ranking independente.
--   Termina com QUALIFY ranking <= 3 filtrando só o top 3 de cada mês.
--
-- Detalhe: RANK pula números em caso de empate (1, 1, 3).
--   Se dois usuários empatarem no 1º lugar, não haverá 2º.
-- -------------------------------------------------------

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
-- QUERY 2 — LAG: Variação diária
--
-- O que faz: Compara o total de pedidos de cada dia com o dia anterior,
--   calculando a variação absoluta e percentual.
--
-- Como funciona:
--   Começa somando pedidos por dia no CTE pedidos_diarios (jan a mar/2023).
--   LAG(total) "olha para a linha anterior" na ordem cronológica e traz o valor do dia anterior.
--   Termina com três colunas: o total do dia, a diferença e o percentual de variação.
--
-- Detalhe: o primeiro dia sempre terá NULL no total_dia_anterior
--   porque não há linha anterior.
-- -------------------------------------------------------

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
-- QUERY 3 — Running Total: Acumulado do mês
--
-- O que faz: Mostra o total de pedidos de cada dia junto com o acumulado
--   crescente do mês.
--
-- Como funciona:
--   Começa somando pedidos por dia (janeiro/2023).
--   SUM(total) OVER (...) soma tudo desde o primeiro dia até o dia atual.
--   Termina com cada linha mostrando: pedidos do dia + total acumulado até aquele dia.
--
-- Detalhe: UNBOUNDED PRECEDING = "desde o começo". CURRENT ROW = "até aqui".
--   Juntos formam uma janela que cresce linha a linha.
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
-- QUERY 4 — PERCENTILE: Distribuição de itens por status
--
-- O que faz: Calcula a mediana e o percentil 95 de itens por pedido,
--   agrupado por status.
--
-- Como funciona:
--   Começa lendo diretamente a tabela de pedidos com LIMIT 1000.
--   PERCENTILE_CONT(num_of_item, 0.5) = mediana (50% dos pedidos têm menos que isso).
--   PERCENTILE_CONT(num_of_item, 0.95) = p95 (95% dos pedidos têm menos que isso).
--   Termina mostrando para cada status: total de pedidos, mediana e p95 de itens.
--
-- Detalhe: a média é distorcida por outliers. Se existem pedidos com 100 itens,
--   a mediana ignora esse efeito — por isso é mais confiável para distribuição.
-- -------------------------------------------------------

SELECT
  status,
  COUNT(*) AS total,
  ROUND(PERCENTILE_CONT(num_of_item, 0.5) OVER (PARTITION BY status), 2) AS mediana_itens,
  ROUND(PERCENTILE_CONT(num_of_item, 0.95) OVER (PARTITION BY status), 2) AS p95_itens
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE num_of_item IS NOT NULL
LIMIT 1000;
