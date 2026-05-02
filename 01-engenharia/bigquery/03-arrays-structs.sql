-- EXERCÍCIO 3: Arrays e Structs no BigQuery
-- Objetivo: trabalhar com tipos complexos — essencial para dados aninhados (JSON, Avro, Parquet)
-- Dataset: bigquery-public-data.thelook_ecommerce

-- -------------------------------------------------------
-- STRUCT — agrupar campos relacionados
-- -------------------------------------------------------

SELECT
  order_id,
  STRUCT(
    user_id AS id,
    gender  AS genero,
    age     AS idade
  ) AS cliente,
  STRUCT(
    status     AS status,
    num_of_item AS qtd_itens,
    created_at  AS criado_em
  ) AS pedido
FROM `bigquery-public-data.thelook_ecommerce.orders`
LIMIT 10;


-- -------------------------------------------------------
-- ARRAY_AGG — agregar valores em array
-- -------------------------------------------------------

-- Todos os status de pedido de cada usuário
SELECT
  user_id,
  ARRAY_AGG(DISTINCT status IGNORE NULLS ORDER BY status) AS status_utilizados,
  COUNT(*) AS total_pedidos
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE user_id IS NOT NULL
GROUP BY user_id
ORDER BY total_pedidos DESC
LIMIT 20;


-- -------------------------------------------------------
-- UNNEST — explodir array em linhas
-- -------------------------------------------------------

WITH usuarios AS (
  SELECT
    user_id,
    ARRAY_AGG(DISTINCT status IGNORE NULLS) AS status_utilizados
  FROM `bigquery-public-data.thelook_ecommerce.orders`
  WHERE user_id IS NOT NULL
  GROUP BY user_id
  LIMIT 5
)
SELECT
  user_id,
  status
FROM usuarios,
UNNEST(status_utilizados) AS status;


-- -------------------------------------------------------
-- JSON_EXTRACT — trabalhar com colunas JSON (string)
-- -------------------------------------------------------

-- Simulando uma coluna JSON (padrão comum em eventos de Pub/Sub)
WITH eventos AS (
  SELECT '{"user_id": 42, "acao": "compra", "tags": ["novo", "promocao"]}' AS payload
)
SELECT
  JSON_EXTRACT_SCALAR(payload, '$.user_id')  AS user_id,
  JSON_EXTRACT_SCALAR(payload, '$.acao')     AS acao,
  JSON_EXTRACT_ARRAY(payload, '$.tags')      AS tags
FROM eventos;
