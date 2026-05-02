-- EXERCÍCIO 5: DML Avançado — MERGE, INSERT, UPDATE, DELETE
-- Objetivo: dominar operações de escrita no BigQuery (essencial para pipelines ELT)
-- Dataset: bigquery-public-data.thelook_ecommerce

-- -------------------------------------------------------
-- Criação das tabelas de destino e staging
-- -------------------------------------------------------

CREATE TABLE IF NOT EXISTS engenharia_bigquery.pedidos_destino
PARTITION BY DATE(created_at)
CLUSTER BY status, gender
AS
SELECT *
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE FALSE;  -- cria estrutura vazia


CREATE TABLE IF NOT EXISTS engenharia_bigquery.pedidos_staging
AS
SELECT *
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE DATE(created_at) = '2023-06-01'
LIMIT 1000;


-- -------------------------------------------------------
-- MERGE — upsert (insert se novo, update se existe)
-- Padrão fundamental em pipelines ELT/CDC
-- -------------------------------------------------------

MERGE engenharia_bigquery.pedidos_destino AS destino
USING engenharia_bigquery.pedidos_staging AS origem
ON destino.order_id = origem.order_id

WHEN MATCHED THEN
  UPDATE SET
    status      = origem.status,
    num_of_item = origem.num_of_item

WHEN NOT MATCHED THEN
  INSERT ROW;


-- -------------------------------------------------------
-- DELETE condicional
-- -------------------------------------------------------

DELETE FROM engenharia_bigquery.pedidos_destino
WHERE status = 'Cancelled';


-- -------------------------------------------------------
-- INSERT seletivo
-- -------------------------------------------------------

INSERT INTO engenharia_bigquery.pedidos_destino
SELECT *
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE DATE(created_at) = '2023-06-02'
LIMIT 500;


-- -------------------------------------------------------
-- Verificar resultado
-- -------------------------------------------------------

SELECT
  DATE(created_at) AS dia,
  status,
  COUNT(*) AS total_pedidos,
  SUM(num_of_item) AS total_itens
FROM engenharia_bigquery.pedidos_destino
GROUP BY 1, 2
ORDER BY 1, 2;
