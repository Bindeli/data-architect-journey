-- EXERCÍCIO 4: Views e Views Materializadas
-- Objetivo: entender quando usar cada tipo e o impacto em custo/performance
-- Dataset: bigquery-public-data.thelook_ecommerce

-- -------------------------------------------------------
-- VIEW COMUM — lógica reutilizável, sem custo de armazenamento
-- Executa a query toda vez que for chamada
-- -------------------------------------------------------

CREATE OR REPLACE VIEW engenharia_bigquery.vw_pedidos_diarios AS
SELECT
  DATE(created_at) AS dia,
  COUNT(*) AS total_pedidos,
  SUM(num_of_item) AS total_itens,
  ROUND(AVG(num_of_item), 2) AS media_itens
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE status != 'Cancelled'
GROUP BY 1;


-- -------------------------------------------------------
-- VIEW MATERIALIZADA — resultado pré-computado e atualizado automaticamente
-- Custo de armazenamento, mas queries muito mais rápidas e baratas
-- -------------------------------------------------------

CREATE MATERIALIZED VIEW engenharia_bigquery.mv_pedidos_mensais
OPTIONS (enable_refresh = true, refresh_interval_minutes = 60)
AS
SELECT
  DATE_TRUNC(created_at, MONTH) AS mes,
  status,
  gender,
  COUNT(*) AS total_pedidos,
  SUM(num_of_item) AS total_itens
FROM `bigquery-public-data.thelook_ecommerce.orders`
GROUP BY 1, 2, 3;


-- -------------------------------------------------------
-- Comparar: query direto vs. query na view materializada
-- -------------------------------------------------------

-- Direto na tabela (escaneia tudo)
SELECT
  DATE_TRUNC(created_at, MONTH) AS mes,
  COUNT(*) AS total_pedidos
FROM `bigquery-public-data.thelook_ecommerce.orders`
GROUP BY 1
ORDER BY 1;

-- Via view materializada (lê resultado pré-computado)
SELECT mes, SUM(total_pedidos) AS total_pedidos
FROM engenharia_bigquery.mv_pedidos_mensais
GROUP BY 1
ORDER BY 1;


-- -------------------------------------------------------
-- Quando usar cada uma?
-- VIEW COMUM    → lógica de negócio, dados sempre frescos, baixo volume
-- VIEW MATER.   → agregações frequentes, alto volume, tolera dados com delay
-- -------------------------------------------------------
