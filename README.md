# data-architect-journey

PDI de evolução de Data Engineer para Data Architect no Google Cloud

---

## Objetivo

Aprofundar competências técnicas de engenharia de dados em Google Cloud e evoluir para visão arquitetural, escalabilidade, padrões e desenho de soluções de dados orientadas a produto.

---

## Roadmap

| Fase | Período | Foco | Status |
|------|---------|------|--------|
| 1 | Mês 1–2 | Engenharia de Dados Avançada | 🔄 Em andamento |
| 2 | Mês 3–5 | Arquitetura de Dados | ⏳ Pendente |
| 3 | Mês 6 | Estratégia e Governança | ⏳ Pendente |

---

## Estrutura do Repositório

```
data-architect-journey/
│
├── 01-engenharia/          # Fase 1 — Base técnica hands-on
│   ├── bigquery/
│   ├── dataflow/
│   ├── pubsub/
│   ├── composer/
│   └── cloud-run-functions/
│
├── 02-arquitetura/         # Fase 2 — Padrões e desenho de soluções
│   ├── padroes/
│   ├── diagramas/
│   └── decisoes/
│
├── 03-estrategia/          # Fase 3 — Dados como produto
│   ├── data-contracts/
│   ├── governanca/
│   └── kpis/
│
├── projetos/               # Projetos práticos obrigatórios
│   ├── projeto-1-lakehouse/
│   ├── projeto-2-event-driven/
│   └── projeto-3-data-product/
│
└── estudos/                # Anotações e resumos de estudo
    └── anotacoes/
```

---

## Projetos Práticos

### Projeto 1 — Lakehouse Completo
> GCS + BigQuery + Dataflow + Pub/Sub

- [ ] Ingestão streaming + batch
- [ ] Transformações no Dataflow
- [ ] Camadas RAW, CURATED, GOLD
- [ ] Orquestração com Composer

### Projeto 2 — Arquitetura Event-Driven
> Pub/Sub + Dataflow + Cloud Run + BigQuery

- [ ] Pub/Sub como backbone
- [ ] Processamento com Dataflow
- [ ] Enriquecimento com Cloud Run
- [ ] Armazenamento em BigQuery + GCS

### Projeto 3 — Data Product
> Produto de dados completo com contrato e governança

- [ ] Contrato de dados
- [ ] SLA definido
- [ ] Dicionário de dados
- [ ] Observabilidade
- [ ] Custo estimado
- [ ] Arquitetura documentada
- [ ] Mapa de componentes

---

## Indicadores de Evolução

- [ ] Redução de custo BigQuery em pipelines críticos
- [ ] Melhoria de performance e latência
- [ ] Arquiteturas documentadas aprovadas em revisões técnicas
- [ ] Entrega de 2+ produtos de dados com contrato e governança
- [ ] Zero falhas críticas de pipeline por desenho inadequado
