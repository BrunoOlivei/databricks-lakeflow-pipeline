CREATE OR REFRESH STREAMING TABLE olist_bronze.orders_items
COMMENT 'Items dos pedidos, como chegaram no landing'
TBLPROPERTIES(
    'quality' = 'bronze',
    'pipelines.reset.allowed' = 'false'
)
AS SELECT 
    *,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '${landing_path}/order_items/',
    format => 'json',
    schema => 'order_id STRING, order_item_id STRING, product_id STRING, seller_id STRING, shipping_limit_date STRING, price STRING, freight_value STRING'
);

CREATE OR REFRESH STREAMING TABLE olist_silver.orders_items (
    CONSTRAINT order_id_presente EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT preco_nao_negativo EXPECT (price >= 0) ON VIOLATION DROP ROW,
    CONSTRAINT produto_na_dimensao EXPECT (product_category_en IS NOT NULL)
)
COMMENT 'Itens tipados e enriquecidos com a categoria do produto'
TBLPROPERTIES (
    'quality' = 'silver'
)
AS SELECT
    i.order_id,
    CAST(i.order_item_id AS INT) AS order_item_id,
    i.product_id,
    i.seller_id,
    to_timestamp(i.shipping_limit_date) AS shipping_limit_date,
    CAST(i.price AS DECIMAL(10, 2)) AS price,
    CAST(i.freight_value AS DECIMAL(10, 2)) AS freight_value,
    p.product_category_en,
    i._ingested_at
FROM STREAM (olist_bronze.orders_items) AS i
LEFT JOIN olist_silver.dim_products AS p
ON i.product_id = p.product_id