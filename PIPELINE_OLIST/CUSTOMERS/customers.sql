CREATE OR REFRESH STREAMING TABLE olist_bronze.customers
COMMENT 'Snapshot do cliente de cada pedido, como chegou no landing'
TBLPROPERTIES (
    'quality' = 'bronze',
    'pipelines.reset.allowed' = 'false'
)
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${landing_path}/customers/',
  format => 'json',
  schema => 'customer_id STRING, customer_unique_id STRING, customer_zip_code_prefix STRING, customer_city STRING, customer_state STRING, snapshot_ts STRING'
);

CREATE OR REFRESH STREAMING TABLE olist_silver.customers (
  CONSTRAINT customer_id_presente EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT snapshot_ts_presente EXPECT (snapshot_ts IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT uf_valida EXPECT (length(customer_state) = 2)
)
COMMENT 'Snapshots de cliente tipados'
TBLPROPERTIES (
    'quality' = 'silver'
)
AS SELECT
  customer_id,
  customer_unique_id,
  lpad(customer_zip_code_prefix, 5, '0') AS customer_zip_code_prefix,
  initcap(customer_city) AS customer_city,
  upper(customer_state) AS customer_state,
  to_timestamp(snapshot_ts) AS snapshot_ts,
  _ingested_at
FROM STREAM(olist_bronze.customers);

CREATE OR REFRESH STREAMING TABLE olist_silver.customers_current
COMMENT 'Cadastro atual de clientes — SCD tipo 1'
TBLPROPERTIES (
    'quality' = 'silver'
);
 
CREATE FLOW customers_current_flow AS AUTO CDC INTO olist_silver.customers_current
FROM STREAM(olist_silver.customers)
KEYS (customer_id)
SEQUENCE BY snapshot_ts
COLUMNS * EXCEPT (_ingested_at)
STORED AS SCD TYPE 1;