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

### Funções principais
| Função | Para que serve |
|--------|---------------|
| `RANK / DENSE_RANK` | Ranking com ou sem empate |
| `ROW_NUMBER` | Numeração única por linha |
| `LAG / LEAD` | Acessar linha anterior/próxima |
| `SUM OVER` | Running total / acumulado |
| `PERCENTILE_CONT` | Mediana, percentis |
| `QUALIFY` | Filtrar resultado de window function |

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

## Conceitos gerais para revisar
- [ ] Partição por data vs. integer range
- [ ] Limite de 4.000 partições por tabela
- [ ] Custo: $5/TB processado (1TB gratuito/mês)
- [ ] Slots: unidade de computação do BigQuery
- [ ] BI Engine: cache em memória para dashboards
- [ ] Row-level security e column-level security
