-- EXERCÍCIO 7: Scheduled Queries e Automação
-- Objetivo: automatizar cargas periódicas diretamente no BigQuery
-- Dataset: bigquery-public-data.thelook_ecommerce

-- -------------------------------------------------------
-- Criar a tabela de destino do snapshot
-- -------------------------------------------------------

CREATE TABLE IF NOT EXISTS engenharia_bigquery.pedidos_snapshot (
  data_carga     DATE,
  dia            DATE,
  status         STRING,
  gender         STRING,
  total_pedidos  INT64,
  total_itens    INT64
)
PARTITION BY data_carga;


-- -------------------------------------------------------
-- Query que será agendada — snapshot diário
-- No console GCP: BigQuery > Scheduled Queries > Create Scheduled Query
-- Frequência: diária às 06:00
-- Parâmetro automático disponível: @run_date (data da execução)
-- -------------------------------------------------------

INSERT INTO `engenharia_bigquery.pedidos_snapshot`
SELECT
  @run_date AS data_carga,
  DATE(created_at) AS dia,
  status,
  gender,
  COUNT(*) AS total_pedidos,
  SUM(num_of_item) AS total_itens
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE DATE(created_at) = DATE_SUB(@run_date, INTERVAL 1 DAY)
GROUP BY 1, 2, 3, 4;


-- -------------------------------------------------------
-- Verificar snapshots carregados
-- -------------------------------------------------------

SELECT
  data_carga,
  COUNT(DISTINCT dia) AS dias_cobertos,
  SUM(total_pedidos) AS pedidos,
  SUM(total_itens) AS itens
FROM engenharia_bigquery.pedidos_snapshot
GROUP BY 1
ORDER BY 1 DESC;
