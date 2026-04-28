# Dataflow (Apache Beam) — Anotações de Estudo

## Exercício 1 — Pipeline Batch

### Conceitos-chave

| Conceito | O que é |
|----------|---------|
| `Pipeline` | O grafo completo de transformações |
| `PCollection` | Coleção de dados imutável que flui entre os passos |
| `PTransform` | Cada operação: `Map`, `Filter`, `GroupByKey`, etc. |
| `Runner` | Onde roda: `DirectRunner` (local) ou `DataflowRunner` (GCP) |
| `DoFn` | Função customizada aplicada a cada elemento |

### Diferença entre DirectRunner e DataflowRunner
- **DirectRunner** — roda na sua máquina, ótimo para testar
- **DataflowRunner** — roda no GCP, escala automaticamente, gera custo

### Resultado do exercício
| Métrica | Valor |
|---------|-------|
| Tempo do job | _preencha_ |
| Workers utilizados | _preencha_ |
| Bytes lidos | _preencha_ |
| Linhas gravadas no BQ | _preencha_ |

### Conclusões
_Escreva aqui o que você observou_

---

## Conceitos para revisar
- [ ] Windowing: Fixed, Sliding, Session windows
- [ ] Triggers e acumulação (streaming)
- [ ] Side inputs
- [ ] Combinar Dataflow com Pub/Sub (streaming — próximo módulo)
