CREATE OR REFRESH STREAMING TABLE olist_bronze.order_events
COMMENT 'Eventos de status dos pedido'
TBLPROPERTIES (
    'quality' = 'bronze',
    'pipelines.reset.allowed' = 'false'
)
AS SELECT 
    *,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '${landing_path}/order_events/',
    format => 'json',
    schema => 'order_id STRING, customer_id STRING, order_status STRING, event_ts STRING, purchase_ts STRING, approved_ts STRING, carrier_ts STRING, delivered_ts STRING, estimated_delivery_ts STRING, operation STRING'
);

CREATE OR REFRESH STREAMING TABLE olist_silver.order_events (
  CONSTRAINT order_id_presente EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW, -- Se order_id for nulo a o registro é descartado
  CONSTRAINT event_ts_presente EXPECT (event_ts IS NOT NULL) ON VIOLATION FAIL UPDATE, -- se event_ts for nulo a transação desta tabela é revertida e as downstream não recebem dados novos o flows independentes continuam.
  CONSTRAINT status_conhecido EXPECT (order_status IN ('created', 'approved', 'invoiced', 'processing', 'shipped', 'delivered', 'canceled', 'unavailable')), -- Se algum valor de order_status não for o da lista o registro é gravado mas um aviso é criado
  CONSTRAINT operacao_valida EXPECT (operation IN ('UPSERT', 'DELETE')) ON VIOLATION DROP ROW -- se operation não for um dos valores o registro é descartado
)
COMMENT 'Eventos de status tipados e validados'
TBLPROPERTIES (
    'quality' = 'silver'
)
AS SELECT
  order_id,
  customer_id,
  order_status,
  to_timestamp(event_ts) AS event_ts,
  to_timestamp(purchase_ts) AS purchase_ts,
  to_timestamp(approved_ts) AS approved_ts,
  to_timestamp(carrier_ts) AS carrier_ts,
  to_timestamp(delivered_ts) AS delivered_ts,
  to_timestamp(estimated_delivery_ts) AS estimated_delivery_ts,
  operation,
  _ingested_at
FROM STREAM(olist_bronze.order_events);

CREATE OR REFRESH STREAMING TABLE olist_silver.orders_current
COMMENT 'Estado atual de cada pedido — SCD tipo 1'
TBLPROPERTIES (
    'quality' = 'silver'
);

CREATE FLOW orders_current_flow AS AUTO CDC INTO olist_silver.orders_current
FROM STREAM(olist_silver.order_events)
KEYS (order_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY event_ts
COLUMNS * EXCEPT (operation, _ingested_at)
STORED AS SCD TYPE 1;


CREATE OR REFRESH STREAMING TABLE olist_silver.orders_status_history
COMMENT 'Histórico de status de cada pedido — SCD tipo 2'
TBLPROPERTIES (
    'quality' = 'silver'
);

CREATE FLOW orders_status_history_flow AS AUTO CDC INTO olist_silver.orders_status_history
FROM STREAM(olist_silver.order_events)
KEYS (order_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY event_ts
COLUMNS * EXCEPT (operation, _ingested_at)
STORED AS SCD TYPE 2
TRACK HISTORY ON order_status;