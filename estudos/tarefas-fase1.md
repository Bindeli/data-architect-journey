# Tarefas — Fase 1: Engenharia de Dados (Mês 1 e 2)

---

## BigQuery Avançado

**T1 — Particionamento e Clustering**
Criar tabela particionada por data e clusterizada por colunas de filtro frequente. Comparar bytes processados antes e depois da otimização. Entender como o BigQuery faz partition pruning e como o clustering reduz o escaneamento dentro de cada partição.

**T2 — Window Functions**
Praticar RANK, LAG/LEAD, running total com SUM OVER e percentis com PERCENTILE_CONT. Entender o padrão CTE para misturar window functions com agregações, e usar QUALIFY para filtrar rankings sem subquery.

**T3 — Arrays e Structs**
Trabalhar com tipos complexos: STRUCT para agrupar campos relacionados, ARRAY_AGG para agregar valores em lista, UNNEST para explodir arrays em linhas, e JSON_EXTRACT para parsear payloads JSON — padrão comum em dados vindos de APIs e Pub/Sub.

**T4 — Views e Views Materializadas**
Criar view comum e view materializada sobre os mesmos dados e comparar bytes processados. Entender quando usar cada uma: view comum para lógica de negócio com dados frescos, view materializada para agregações frequentes em alto volume.

**T5 — DML Avançado (MERGE / Upsert)**
Implementar o padrão MERGE para upsert — insert se o registro não existe, update se existe. Praticar DELETE condicional e INSERT seletivo. Esse padrão é fundamental em pipelines CDC (Change Data Capture).

**T6 — Custo e Otimização**
Usar INFORMATION_SCHEMA para inspecionar tabelas, partições e jobs executados. Comparar queries com SELECT * vs. colunas específicas, e filtros que usam vs. ignoram partição. Estimar custo com dry run via CLI antes de executar queries pesadas.

**T7 — Scheduled Queries**
Automatizar uma carga diária usando Scheduled Queries do BigQuery. Usar o parâmetro @run_date para processar sempre o dia anterior. Criar tabela de destino particionada por data de carga para facilitar reprocessamento.

**T8 — Joins Avançados**
Entender Broadcast Join vs. Shuffle Join e como o BigQuery decide qual usar. Praticar EXISTS e NOT EXISTS como alternativas mais eficientes ao JOIN quando só precisa confirmar existência. Identificar e tratar skew de dados em chaves de join.

---

## Dataflow (Apache Beam)

**T9 — Pipeline Batch (CSV → BigQuery)**
Construir um pipeline Apache Beam que lê um arquivo CSV do GCS, aplica transformações (parse, filtro, mapeamento) e grava no BigQuery. Rodar primeiro com DirectRunner local para validar, depois subir no DataflowRunner no GCP e analisar o job no console.

**T10 — Pipeline Streaming (Pub/Sub → BigQuery)**
Construir um pipeline streaming que consome mensagens do Pub/Sub em tempo real, transforma os dados e grava no BigQuery com janela de tempo (windowing). Entender a diferença entre Fixed Window, Sliding Window e Session Window.

**T11 — Monitoramento de Jobs**
Analisar o gráfico de execução de um job Dataflow: identificar os estágios (Read, Compute, Shuffle, Write), medir throughput por estágio, e identificar gargalos. Praticar ajuste de número de workers e uso de Autoscaling.

---

## Pub/Sub

**T12 — Publicar e Consumir Mensagens**
Criar um tópico e uma subscription no Pub/Sub. Publicar mensagens via Python SDK e consumi-las com pull e push. Entender a diferença entre os dois modos e quando usar cada um.

**T13 — Padrões de Arquitetura**
Implementar o padrão fan-out: um publisher, múltiplos subscribers independentes processando o mesmo evento. Configurar Dead Letter Queue (DLQ) para mensagens que falham repetidamente. Entender garantias de entrega: at-least-once e ordenação por chave.

**T14 — Integração com Dataflow**
Conectar Pub/Sub como fonte de um pipeline Dataflow streaming. Entender como funciona o acknowledge de mensagens e o que acontece se o pipeline falhar antes de confirmar o processamento.

---

## Composer (Airflow)

**T15 — Criar Ambiente e Primeira DAG**
Criar um ambiente Composer 2 no GCP. Fazer deploy de uma DAG simples com dois tasks sequenciais via gsutil para a pasta dags/ do bucket. Verificar execução na interface do Airflow.

**T16 — DAG com Operadores GCP**
Construir uma DAG que orquestra um pipeline completo: acionar um job Dataflow, aguardar conclusão, rodar uma query no BigQuery e enviar alerta em caso de falha. Usar BigQueryOperator, DataflowOperator e EmailOperator.

**T17 — Retries, SLAs e Alertas**
Configurar retries com backoff exponencial, definir SLA por task e criar callback de alerta quando o SLA é violado. Entender como o Airflow trata dependências entre tasks e como usar XCom para passar dados entre elas.

---

## Cloud Run + Functions

**T18 — Cloud Function por Evento**
Criar uma Cloud Function acionada por evento no GCS: quando um arquivo novo chega em um bucket, a função processa e publica uma mensagem no Pub/Sub. Praticar deploy via gcloud e monitoramento de logs no Cloud Logging.

**T19 — API com Cloud Run**
Criar uma API leve em Python (FastAPI) containerizada e deployada no Cloud Run. A API recebe um payload JSON, aplica uma transformação e retorna o resultado — padrão de enriquecimento de dados em pipelines event-driven.

**T20 — Integração no Pipeline**
Conectar Cloud Run como etapa de enriquecimento dentro de um pipeline: Pub/Sub aciona o Cloud Run, que enriquece o dado e publica de volta no Pub/Sub para o Dataflow processar e gravar no BigQuery.
