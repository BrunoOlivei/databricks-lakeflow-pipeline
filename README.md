# Databricks Lakeflow Pipeline — Olist

Projeto de estudo de engenharia de dados no **Azure Databricks**, construído para praticar de ponta a ponta os temas do exame **Databricks Certified Data Engineer Associate**: ingestão incremental com Auto Loader, pipelines declarativos (Lakeflow Spark Declarative Pipelines), qualidade de dados com expectations, CDC com SCD tipo 1 e 2, orquestração com Lakeflow Jobs e versionamento com Git folders.

Os dados vêm do dataset público **Brazilian E-Commerce by Olist** (Kaggle). Um notebook gerador transforma os CSVs originais em arquivos JSON diários, simulando a chegada contínua de dados de um e-commerce.

## Stack

- Azure Databricks (Premium) com Unity Catalog
- Lakeflow Spark Declarative Pipelines (SQL), compute serverless, modo triggered
- Auto Loader (`read_files` em streaming tables)
- `AUTO CDC` (SCD tipo 1 e tipo 2)
- Lakeflow Jobs com trigger de file arrival, If/else, Run if e task values
- Unity Catalog Volumes como landing zone
- Git folders + GitHub

## Arquitetura

```mermaid
graph LR
  CSV[(CSVs Kaggle<br/>landing/source)] --> GEN[01_gerador_olist]
  GEN --> V[(Volume landing<br/>JSON diários)]

  subgraph Pipeline declarativo
    V --> B1[bronze.order_events]
    V --> B2[bronze.orders_items]
    V --> B3[bronze.payments]
    V --> B4[bronze.reviews]
    V --> B5[bronze.customers]
    V --> D1[silver.dim_products MV]
    V --> D2[silver.dim_sellers MV]

    B1 --> S1[silver.order_events]
    S1 -- AUTO CDC SCD 1 --> C1[silver.orders_current]
    S1 -- AUTO CDC SCD 2 --> C2[silver.orders_status_history]
    B2 --> S2[silver.orders_items]
    D1 -. stream-static join .-> S2
    B3 --> S3[silver.payments]
    B4 --> S4[silver.reviews]
    B5 --> S5[silver.customers]
    S5 -- AUTO CDC SCD 1 --> C3[silver.customers_current]

    C1 --> G1[gold.receita_diaria_uf]
    C3 --> G1
    S3 --> G1
    C1 --> G2[gold.desempenho_entrega_uf]
    C3 --> G2
    S2 --> G3[gold.avaliacao_por_categoria]
    S4 --> G3
  end
```

## Estrutura do repositório

```
.
├── 00_setup.py              # cria schemas (olist_raw, olist_bronze, olist_silver, olist_gold) e o Volume de landing
├── 01_gerador_olist.py      # gera N dias de JSON por execução a partir dos CSVs do Kaggle
├── 02_checagem_gold.py      # task do job: publica task values a partir da gold
├── 03_alerta.py             # task do job: alerta genérico (ramo false do If/else e Run if)
└── PIPELINE_OLIST/          # código-fonte do pipeline declarativo (SQL)
    ├── ORDERS/
    │   ├── orders_events.sql   # bronze + silver de eventos, CDC SCD 1 e SCD 2
    │   └── orders_items.sql    # bronze + silver de itens, join com dim_products
    ├── PAYMENTS/payments.sql
    ├── REVIEWS/reviews.sql
    ├── CUSTOMERS/customers.sql # bronze + silver de clientes, CDC SCD 1
    ├── DIMENSOES/dimensoes.sql # materialized views de produtos e vendedores
    └── GOLD/gold.sql           # materialized views de negócio
```

## Camadas

### Bronze — `olist_bronze`

Streaming tables com Auto Loader (`STREAM read_files(...)`, formato JSON, schema explícito). Cada linha recebe `_source_file` (de `_metadata.file_path`) e `_ingested_at`. Todas têm `pipelines.reset.allowed = false`, para que um full refresh não apague o histórico ingerido.

| Tabela | Origem no Volume |
|---|---|
| `order_events` | `order_events/` |
| `orders_items` | `order_items/` |
| `payments` | `payments/` |
| `reviews` | `reviews/` |
| `customers` | `customers/` |

### Silver — `olist_silver`

| Tabela | Tipo | O que faz |
|---|---|---|
| `order_events` | Streaming table | Tipagem dos timestamps e expectations |
| `orders_current` | Streaming table (`AUTO CDC`) | Estado atual de cada pedido — SCD tipo 1, com `APPLY AS DELETE WHEN operation = 'DELETE'` |
| `orders_status_history` | Streaming table (`AUTO CDC`) | Histórico de status — SCD tipo 2 com `TRACK HISTORY ON order_status` |
| `orders_items` | Streaming table | Itens tipados (`DECIMAL`) e enriquecidos com a categoria via stream-static join com `dim_products` |
| `payments` | Streaming table | Pagamentos tipados (`DECIMAL`) |
| `reviews` | Streaming table | Avaliações tipadas |
| `customers` | Streaming table | Snapshots de cliente tipados |
| `customers_current` | Streaming table (`AUTO CDC`) | Cadastro atual de clientes — SCD tipo 1 |
| `dim_products` | Materialized view | Produtos com a categoria traduzida para inglês |
| `dim_sellers` | Materialized view | Vendedores |

### Gold — `olist_gold`

| Materialized view | Conteúdo |
|---|---|
| `receita_diaria_uf` | Pedidos e receita por dia de compra e UF (exclui cancelados e indisponíveis) |
| `desempenho_entrega_uf` | Pedidos entregues por UF, prazo médio de entrega e percentual com atraso |
| `avaliacao_por_categoria` | Nota média das avaliações por categoria de produto |

## Qualidade de dados

As expectations cobrem as três ações possíveis:

| Tabela | Regra | Ação |
|---|---|---|
| `order_events` | `order_id IS NOT NULL` | `DROP ROW` |
| `order_events` | `event_ts IS NOT NULL` | `FAIL UPDATE` |
| `order_events` | `order_status` em lista conhecida | `WARN` |
| `order_events` | `operation IN ('UPSERT', 'DELETE')` | `DROP ROW` |
| `orders_items` | `order_id IS NOT NULL`, `price >= 0` | `DROP ROW` |
| `orders_items` | produto encontrado na dimensão | `WARN` |
| `payments` | `order_id IS NOT NULL`, `payment_value >= 0` | `DROP ROW` |
| `reviews` | `order_id IS NOT NULL` | `DROP ROW` |
| `reviews` | `review_score BETWEEN 1 AND 5` | `WARN` |
| `customers` | `customer_id IS NOT NULL`, `snapshot_ts IS NOT NULL` | `DROP ROW` |
| `customers` | UF com 2 caracteres | `WARN` |

Em pipeline triggered, um `FAIL UPDATE` reverte a transação do fluxo que violou a regra. As tabelas downstream não recebem os dados novos daquele update, e os fluxos independentes seguem normalmente. O registro inválido continua na bronze e é relido na execução seguinte.

## Orquestração

O job `OLIST_JOB` é disparado por **file arrival** na pasta `landing/order_events/` (a última que o gerador grava), com espera de 60 s após a última alteração. Fila habilitada e notificação por e-mail em caso de falha.

| Task | Tipo | Depende de | Condição |
|---|---|---|---|
| `pipeline_olist` | Pipeline | — | — |
| `checagem_gold` | Notebook `02_checagem_gold` | `pipeline_olist` | Publica os task values `ultima_data` e `pedidos_ultimo_dia` |
| `tem_pedidos` | If/else | `checagem_gold` | `{{tasks.checagem_gold.values.pedidos_ultimo_dia}} > 0` |
| `alerta_sem_pedidos` | Notebook `03_alerta` | `tem_pedidos` (false) | — |
| `alerta_falha` | Notebook `03_alerta` | `pipeline_olist`, `checagem_gold` | Run if: **At least one failed** |

## Como executar

### Pré-requisitos

- Workspace Databricks com Unity Catalog e compute serverless
- Permissão para criar schemas, Volumes e tabelas num catálogo (o código usa `databricksestudos`; troque pelo seu)

### Passo a passo

1. Clone este repositório como **Git folder** no workspace.
2. Rode `00_setup.py` para criar os schemas e o Volume `olist_raw.landing`.
3. Baixe o dataset no [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), descompacte e faça upload dos CSVs para `/Volumes/<catalogo>/olist_raw/landing/source/`.
4. Rode `01_gerador_olist.py`. Widgets:

| Widget | Padrão | Uso |
|---|---|---|
| `data_inicial` | `2017-01-01` | Primeiro dia gerado |
| `dias_por_execucao` | `1` | Quantos dias gerar por execução |
| `injetar` | `nenhum` | `warn_drop` ou `fail` para testar as expectations |
| `reiniciar` | `nao` | `sim` apaga os arquivos gerados (exige full refresh do pipeline depois) |

5. Crie o pipeline (*Jobs & Pipelines → Create → ETL pipeline*):

| Configuração | Valor |
|---|---|
| Source code | pasta `PIPELINE_OLIST/` |
| Default catalog | seu catálogo |
| Compute | Serverless |
| Pipeline mode | Triggered |
| Configuration | `landing_path` = `/Volumes/<catalogo>/olist_raw/landing` |

6. Crie o job conforme a tabela de orquestração. Ao rodar o gerador de novo, o trigger dispara o job sozinho.

### Cenários de teste

- **`injetar = warn_drop`**: gera evento sem `order_id`, item com preço negativo, avaliação com nota 7 e um pedido de teste que é criado e depois deletado. Confira a aba *Data quality* e o fechamento do registro em `orders_status_history`.
- **`injetar = fail`**: gera evento com `event_ts` nulo. O pipeline falha, `alerta_falha` executa e o job pode ser corrigido com **Repair run**.

### Custos

Use sempre pipeline triggered (nunca contínuo) e pause o trigger do job quando não estiver usando.

## Limitações conhecidas

- A checagem da gold sempre encontra pedidos no último dia presente na tabela, então o ramo false do If/else (`alerta_sem_pedidos`) não é exercitado na prática.
- `avaliacao_por_categoria` calcula a média após o join com os itens, então pedidos com mais itens de uma categoria pesam mais na nota média.
- `receita_diaria_uf` usa a UF atual do cliente (SCD tipo 1), não a UF da época da compra.
- `dim_sellers` ainda não é usada na gold.
- As materialized views de dimensão leem arquivos direto do Volume, o que impede refresh incremental. Como são pequenas e estáticas, o recompute completo é aceitável.
- A definição do job e as configurações do pipeline ainda não estão versionadas neste repositório.

## Próximos passos

- Empacotar pipeline e job em Declarative Automation Bundles (`databricks.yml`) com targets dev e prod.
- Governança no Unity Catalog: row filter por UF e column mask em dados de cliente.
- Análise de desempenho no Spark UI e no query profile.

## Dados e licença

O dataset **Brazilian E-Commerce Public Dataset by Olist** não está incluído neste repositório. Baixe-o diretamente no [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) e respeite a licença indicada na página do dataset.
