# BigQuery — Anotações de Estudo

## Exercício 1 — Particionamento e Clustering

### O que é particionamento?
> Divide a tabela em segmentos por uma coluna (geralmente data). O BigQuery lê **só a partição relevante**, ignorando o resto.

### O que é clustering?
> Ordena os dados dentro de cada partição por até 4 colunas. Reduz ainda mais os bytes lidos em queries com filtro nessas colunas.

### Resultado do exercício
| Query | Bytes processados | Tempo |
|-------|------------------|-------|
| Sem otimização | _preencha após rodar_ | _preencha_ |
| Com partição + cluster | _preencha após rodar_ | _preencha_ |

### Conclusões
_Escreva aqui o que você observou_

---

## Conceitos para revisar
- [ ] Partição por data vs. por coluna inteira (integer range)
- [ ] Limite de partições por tabela (4.000)
- [ ] Custo: $5 por TB processado (após 1TB gratuito/mês)
- [ ] `INFORMATION_SCHEMA.PARTITIONS` — como inspecionar partições
