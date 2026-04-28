-- EXERCÍCIO 8: Joins Avançados e Otimização
-- Objetivo: entender Shuffle Join vs Broadcast Join, EXISTS, NOT EXISTS e anti-patterns

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
SELECT t.trip_id, t.fare, z.zone_name
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips` AS t   -- tabela grande (esquerda)
JOIN (
  SELECT DISTINCT community_area, zone_name
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  WHERE zone_name IS NOT NULL
  LIMIT 100
) AS z                                                            -- tabela pequena (direita)
ON t.pickup_community_area = z.community_area
WHERE DATE(t.trip_start_timestamp) = '2023-06-01';


-- ================================================================
-- 2. EXISTS — verificar existência sem retornar dados da subquery
-- ================================================================
-- Quando usar: checar se um registro existe em outra tabela
-- Vantagem: para assim que encontra o primeiro match (short-circuit)
-- Evita duplicação de linhas (ao contrário do JOIN)

-- "Quais datas têm corridas com pagamento em dinheiro?"
SELECT DISTINCT DATE(t.trip_start_timestamp) AS dia
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips` AS t
WHERE EXISTS (
  SELECT 1
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips` AS sub
  WHERE sub.payment_type = 'Cash'
    AND DATE(sub.trip_start_timestamp) = DATE(t.trip_start_timestamp)
)
ORDER BY dia;

-- Equivalente com JOIN — mas pode gerar duplicatas se não houver DISTINCT
-- e escaneia mais dados pois não faz short-circuit
SELECT DISTINCT DATE(t.trip_start_timestamp) AS dia
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips` AS t
JOIN `bigquery-public-data.chicago_taxi_trips.taxi_trips` AS sub
  ON DATE(sub.trip_start_timestamp) = DATE(t.trip_start_timestamp)
 AND sub.payment_type = 'Cash'
ORDER BY dia;


-- ================================================================
-- 3. NOT EXISTS — anti-join (registros sem correspondência)
-- ================================================================
-- Quando usar: encontrar registros em A que NÃO existem em B
-- Vantagem sobre NOT IN: funciona corretamente com NULLs

-- "Motoristas que nunca usaram cartão de crédito"
SELECT DISTINCT taxi_id
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips` AS t
WHERE taxi_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips` AS sub
    WHERE sub.taxi_id = t.taxi_id
      AND sub.payment_type = 'Credit Card'
  )
LIMIT 20;


-- ================================================================
-- 4. NOT IN — cuidado com NULLs!
-- ================================================================
-- PROBLEMA: se a subquery retornar qualquer NULL,
-- NOT IN retorna FALSE para todas as linhas (resultado vazio)
-- → É um bug silencioso muito comum

-- PERIGOSO — pode retornar vazio se houver NULL em payment_type
SELECT COUNT(*) AS total
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE payment_type NOT IN (
  SELECT payment_type
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  WHERE fare > 100
  -- se algum payment_type for NULL aqui, o NOT IN quebra
);

-- SEGURO — use NOT EXISTS ou filtre NULLs explicitamente
SELECT COUNT(*) AS total
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE payment_type NOT IN (
  SELECT payment_type
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  WHERE fare > 100
    AND payment_type IS NOT NULL  -- proteção explícita
);


-- ================================================================
-- 5. CROSS JOIN — produto cartesiano (use com cuidado)
-- ================================================================
-- Multiplica cada linha de A com cada linha de B
-- Útil para gerar combinações, calendários, grades
-- PERIGOSO em tabelas grandes — explode o volume de dados

-- Caso válido: gerar uma grade de datas x tipos de pagamento
WITH datas AS (
  SELECT DATE_ADD('2023-01-01', INTERVAL n DAY) AS dia
  FROM UNNEST(GENERATE_ARRAY(0, 6)) AS n      -- 7 dias
),
tipos AS (
  SELECT DISTINCT payment_type
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  WHERE payment_type IS NOT NULL
)
SELECT d.dia, t.payment_type
FROM datas AS d
CROSS JOIN tipos AS t
ORDER BY d.dia, t.payment_type;


-- ================================================================
-- 6. SKEW de dados — o problema silencioso dos joins
-- ================================================================
-- Ocorre quando uma chave de join concentra muito mais linhas que outras
-- → Um worker fica sobrecarregado enquanto os outros ficam ociosos
-- → Job fica lento mesmo com muitos workers

-- Identificar skew: distribuição de linhas por chave
SELECT
  pickup_community_area,
  COUNT(*) AS total_corridas,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_total
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE pickup_community_area IS NOT NULL
GROUP BY 1
ORDER BY total_corridas DESC
LIMIT 20;

-- Se uma chave representa >30% das linhas → risco de skew
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
