-- EXERCÍCIO 8: Joins Avançados e Otimização
-- Objetivo: entender Shuffle Join vs Broadcast Join, EXISTS, NOT EXISTS e anti-patterns
-- Dataset: bigquery-public-data.thelook_ecommerce

-- ================================================================
-- 1. BROADCAST JOIN vs SHUFFLE JOIN
-- ================================================================
-- BigQuery decide automaticamente o tipo de join com base no tamanho das tabelas.
--
-- BROADCAST JOIN (eficiente):
--   → Acontece quando uma das tabelas cabe em memória (~100MB por worker)
--   → O BigQuery envia a tabela menor para todos os workers
--   → Sem custo de shuffle (movimentação de dados entre workers)
--
-- SHUFFLE JOIN (custoso):
--   → Acontece quando AMBAS as tabelas são grandes
--   → O BigQuery redistribui os dados entre workers pela chave de join
--   → Gera I/O de rede intenso — é o principal gargalo em joins grandes
--
-- REGRA: coloque sempre a tabela MENOR no lado direito do JOIN


-- BOM: tabela menor à direita → favorece broadcast
SELECT o.order_id, o.status, u.country, u.age
FROM `bigquery-public-data.thelook_ecommerce.orders` AS o          -- tabela grande (esquerda)
JOIN (
  SELECT DISTINCT id, country, age
  FROM `bigquery-public-data.thelook_ecommerce.users`
  WHERE country = 'Brasil'
) AS u                                                              -- tabela pequena (direita)
ON o.user_id = u.id
WHERE DATE(o.created_at) = '2023-06-01';


-- ================================================================
-- 2. EXISTS — verificar existência sem retornar dados da subquery
-- ================================================================
-- Quando usar: checar se um registro existe em outra tabela
-- Vantagem: para assim que encontra o primeiro match (short-circuit)
-- Evita duplicação de linhas (ao contrário do JOIN)

-- "Quais datas têm pedidos com status Returned?"
SELECT DISTINCT DATE(o.created_at) AS dia
FROM `bigquery-public-data.thelook_ecommerce.orders` AS o
WHERE EXISTS (
  SELECT 1
  FROM `bigquery-public-data.thelook_ecommerce.orders` AS sub
  WHERE sub.status = 'Returned'
    AND DATE(sub.created_at) = DATE(o.created_at)
)
ORDER BY dia;

-- Equivalente com JOIN — pode gerar duplicatas e escaneia mais dados
SELECT DISTINCT DATE(o.created_at) AS dia
FROM `bigquery-public-data.thelook_ecommerce.orders` AS o
JOIN `bigquery-public-data.thelook_ecommerce.orders` AS sub
  ON DATE(sub.created_at) = DATE(o.created_at)
 AND sub.status = 'Returned'
ORDER BY dia;


-- ================================================================
-- 3. NOT EXISTS — anti-join (registros sem correspondência)
-- ================================================================
-- Quando usar: encontrar registros em A que NÃO existem em B
-- Vantagem sobre NOT IN: funciona corretamente com NULLs

-- "Usuários que nunca tiveram pedido cancelado"
SELECT DISTINCT user_id
FROM `bigquery-public-data.thelook_ecommerce.orders` AS o
WHERE user_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM `bigquery-public-data.thelook_ecommerce.orders` AS sub
    WHERE sub.user_id = o.user_id
      AND sub.status = 'Cancelled'
  )
LIMIT 20;


-- ================================================================
-- 4. NOT IN — cuidado com NULLs!
-- ================================================================
-- PROBLEMA: se a subquery retornar qualquer NULL,
-- NOT IN retorna FALSE para todas as linhas (resultado vazio silencioso)

-- PERIGOSO — pode retornar vazio se houver NULL em status
SELECT COUNT(*) AS total
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE status NOT IN (
  SELECT status
  FROM `bigquery-public-data.thelook_ecommerce.orders`
  WHERE num_of_item > 5
);

-- SEGURO — filtre NULLs explicitamente
SELECT COUNT(*) AS total
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE status NOT IN (
  SELECT status
  FROM `bigquery-public-data.thelook_ecommerce.orders`
  WHERE num_of_item > 5
    AND status IS NOT NULL  -- proteção explícita
);


-- ================================================================
-- 5. CROSS JOIN — produto cartesiano (use com cuidado)
-- ================================================================
-- Útil para gerar combinações, calendários, grades
-- PERIGOSO em tabelas grandes — explode o volume de dados

-- Caso válido: grade de datas x status de pedido
WITH datas AS (
  SELECT DATE_ADD('2023-01-01', INTERVAL n DAY) AS dia
  FROM UNNEST(GENERATE_ARRAY(0, 6)) AS n
),
status_list AS (
  SELECT DISTINCT status
  FROM `bigquery-public-data.thelook_ecommerce.orders`
  WHERE status IS NOT NULL
)
SELECT d.dia, s.status
FROM datas AS d
CROSS JOIN status_list AS s
ORDER BY d.dia, s.status;


-- ================================================================
-- 6. SKEW de dados — o problema silencioso dos joins
-- ================================================================
-- Ocorre quando uma chave de join concentra muito mais linhas que outras

-- Identificar skew: distribuição de pedidos por usuário
SELECT
  user_id,
  COUNT(*) AS total_pedidos,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 4) AS pct_total
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE user_id IS NOT NULL
GROUP BY 1
ORDER BY total_pedidos DESC
LIMIT 20;

-- Se um user_id representa >30% das linhas → risco de skew no join
-- Solução: filtrar a chave problemática e processar separadamente


-- ================================================================
-- RESUMO: quando usar cada abordagem
-- ================================================================
--
-- JOIN       → quando precisa de colunas de ambas as tabelas
-- EXISTS     → quando só precisa confirmar existência (mais eficiente)
-- NOT EXISTS → anti-join seguro com NULLs
-- NOT IN     → evitar, ou sempre filtrar NULLs explicitamente
-- CROSS JOIN → só para volumes controlados (tabelas pequenas)
--
-- Broadcast: tabela menor sempre à direita
-- Shuffle:   inevitável com 2 tabelas grandes — minimize colunas no join
-- ================================================================
