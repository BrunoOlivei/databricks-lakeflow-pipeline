CREATE OR REFRESH STREAMING TABLE olist_bronze.payments 
COMMENT 'Pagamentos dos pedidos, como chegaram no landing'
TBLPROPERTIES (
    'quality' = 'bronze',
    'pipelines.reset.allowed' = 'false'
)
AS SELECT 
    *,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '${landing_path}/payments',
    format => 'json',
    schema => 'order_id STRING, payment_sequential STRING, payment_type STRING, payment_installments STRING, payment_value STRING'
);

CREATE OR REFRESH STREAMING TABLE olist_silver.payments (
    CONSTRAINT order_id_presente EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valor_nao_negativo EXPECT (payment_value >= 0) ON VIOLATION DROP ROW
)
COMMENT 'Pagamentos tipados'
TBLPROPERTIES (
    'quality' = 'silver'
)
AS SELECT
    order_id,
    CAST(payment_sequential AS INT) AS payment_sequential,
    payment_type,
    CAST(payment_installments AS INT) AS payment_installments,
    CAST(payment_value AS DECIMAL(10,2)) AS payment_value,
    _ingested_at
FROM STREAM olist_bronze.payments