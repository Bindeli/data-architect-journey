# BigQuery Avançado — Anotações de Estudo

## Currículo completo

| # | Exercício | Arquivo | Status |
|---|-----------|---------|--------|
| 1 | Particionamento e Clustering | `01-particao-clustering.sql` | ⏳ |
| 2 | Window Functions | `02-window-functions.sql` | ⏳ |
| 3 | Arrays e Structs | `03-arrays-structs.sql` | ⏳ |
| 4 | Views e Views Materializadas | `04-views-materializadas.sql` | ⏳ |
| 5 | DML Avançado — MERGE / Upsert | `05-dml-merge.sql` | ⏳ |
| 6 | Custo e Otimização de Queries | `06-custo-otimizacao.sql` | ⏳ |
| 7 | Scheduled Queries e Automação | `07-scheduled-query.sql` | ⏳ |
| 8 | Joins Avançados e Otimização | `08-joins-otimizacao.sql` | ⏳ |

---

## Exercício 1 — Particionamento e Clustering

### O que é particionamento?
> Divide a tabela em segmentos por uma coluna (geralmente data). O BigQuery lê **só a partição relevante**, ignorando o resto.

### O que é clustering?
> Ordena os dados dentro de cada partição por até 4 colunas. Reduz ainda mais os bytes lidos em queries com filtro nessas colunas.

### Resultado
| Query | Bytes processados | Tempo |
|-------|------------------|-------|
| Sem otimização | _preencha_ | _preencha_ |
| Com partição + cluster | _preencha_ | _preencha_ |

---

## Exercício 2 — Window Functions

### RANK()
Atribui uma posição a cada linha dentro de uma partição. Em caso de empate, pula o próximo número (1, 1, 3).
```sql
RANK() OVER (PARTITION BY mes ORDER BY total_pedidos DESC)
```
> Use `DENSE_RANK()` se não quiser pular números no empate. Use `QUALIFY ranking <= N` para filtrar o top N sem subquery.

---

### LAG()
Acessa o valor de uma linha anterior na ordem definida. Útil para calcular variação em relação ao período anterior.
```sql
LAG(total) OVER (ORDER BY dia)
-- retorna o valor de "total" da linha anterior
-- primeira linha retorna NULL (sem linha anterior)
```
> `LEAD()` faz o oposto — acessa a próxima linha. Ambos aceitam um segundo parâmetro de offset: `LAG(total, 7)` = 7 linhas atrás.

---

### SUM() OVER — Running Total
Calcula um acumulado crescente linha a linha. A cláusula `ROWS BETWEEN` define a janela de linhas incluídas no cálculo.
```sql
SUM(total) OVER (ORDER BY dia ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
-- UNBOUNDED PRECEDING = desde o início
-- CURRENT ROW         = até a linha atual
```
> Sem o `ROWS BETWEEN`, o BigQuery usa a janela padrão que pode variar. Sempre declare explicitamente para evitar comportamento inesperado.

---

### PERCENTILE_CONT()
Calcula percentis contínuos (interpola entre valores). Útil para mediana e análise de distribuição, que a média não representa bem quando há outliers.
```sql
PERCENTILE_CONT(num_of_item, 0.5) OVER (PARTITION BY status)
-- 0.5 = mediana (50% dos valores estão abaixo)
-- 0.95 = p95 (95% dos valores estão abaixo)
```
> Não pode ser misturado com `COUNT(*)` no mesmo SELECT. Use um CTE: calcule o percentil no CTE, depois agrupe com `MAX()` no SELECT final.

---

### QUALIFY
Filtra o resultado de uma window function sem precisar de subquery. Equivale a um `WHERE` aplicado após o cálculo da window function.
```sql
QUALIFY ranking <= 3
-- equivalente a: SELECT * FROM (...) WHERE ranking <= 3
```
> Só funciona no BigQuery e alguns outros dialetos modernos. Não existe no SQL padrão.

---

### Padrão CTE para window functions com agregação
Quando precisar misturar `COUNT(*)` (que exige `GROUP BY`) com window functions no mesmo resultado:
```sql
WITH base AS (
  SELECT coluna, RANK() OVER (...) AS ranking  -- window function aqui
  FROM tabela
)
SELECT coluna, COUNT(*), MAX(ranking)           -- agregação aqui
FROM base
GROUP BY coluna
```

### Resultado
_Escreva aqui o que você observou_

---

## Exercício 3 — Arrays e Structs

### Conceitos
- **STRUCT** → agrupa campos relacionados (como um objeto/JSON aninhado)
- **ARRAY** → lista de valores no mesmo campo
- **ARRAY_AGG** → agrega valores em array durante GROUP BY
- **UNNEST** → explode array em linhas separadas
- **JSON_EXTRACT** → lê campos de colunas JSON (string)

### Quando usar?
> Dados vindos de APIs, Pub/Sub, Avro, Parquet — geralmente chegam com campos aninhados. Dominar STRUCT e ARRAY é obrigatório para trabalhar com esses formatos.

---

## Exercício 4 — Views e Views Materializadas

### Quando usar cada uma?
| Tipo | Custo armazenamento | Dados frescos | Ideal para |
|------|--------------------:|:-------------:|------------|
| View comum | Nenhum | Sempre | Lógica de negócio, baixo volume |
| View materializada | Sim | Com delay | Agregações frequentes, alto volume |

---

## Exercício 5 — DML Avançado (MERGE)

### MERGE — padrão de upsert em pipelines ELT
```sql
MERGE destino USING origem ON chave
WHEN MATCHED THEN UPDATE ...
WHEN NOT MATCHED THEN INSERT ROW
```
> Fundamental para pipelines CDC (Change Data Capture)

---

## Exercício 6 — Custo e Otimização

### Regras de ouro
- Nunca use `SELECT *` em produção
- Filtre sempre pela coluna de partição
- Use `--dry_run` para estimar custo antes de rodar
- Inspecione jobs caros com `INFORMATION_SCHEMA.JOBS_BY_PROJECT`
- Prefira colunas do tipo `DATE` para particionar, não `TIMESTAMP`

---

## Exercício 7 — Scheduled Queries

### Como agendar
1. BigQuery Console → Scheduled Queries → Create
2. Defina a frequência (ex: diária às 06:00)
3. Use `@run_date` como parâmetro da data de execução
4. Grave sempre em tabela particionada para facilitar reprocessamento

---

## Exercício 8 — Joins Avançados e Otimização

### Broadcast Join vs Shuffle Join
| Tipo | Quando ocorre | Custo |
|------|--------------|-------|
| Broadcast | Tabela direita pequena (~100MB) | Baixo — sem shuffle |
| Shuffle | Ambas as tabelas grandes | Alto — move dados entre workers |

> Regra: coloque sempre a tabela **menor à direita** do JOIN.

### EXISTS vs JOIN vs NOT IN
| Abordagem | Usar quando | Cuidado |
|-----------|------------|---------|
| `EXISTS` | Só precisa confirmar existência | Mais eficiente que JOIN para isso |
| `NOT EXISTS` | Anti-join (registros sem match) | Seguro com NULLs |
| `NOT IN` | Evitar | Retorna vazio silenciosamente se houver NULL na subquery |
| `JOIN` | Precisa de colunas de ambas as tabelas | Pode duplicar linhas |

### Skew de dados
> Ocorre quando uma chave de join concentra muito mais linhas que outras. Um worker fica sobrecarregado e o job fica lento. Identifique com `COUNT(*) GROUP BY chave` e filtre chaves com >30% do volume.

---

## Conceitos gerais para revisar
- [ ] Partição por data vs. integer range
- [ ] Limite de 4.000 partições por tabela
- [ ] Custo: $5/TB processado (1TB gratuito/mês)
- [ ] Slots: unidade de computação do BigQuery
- [ ] BI Engine: cache em memória para dashboards
- [ ] Row-level security e column-level security
