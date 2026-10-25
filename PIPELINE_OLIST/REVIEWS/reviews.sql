CREATE OR REFRESH STREAMING TABLE olist_bronze.reviews
COMMENT 'Avaliações dos pedidos, como chegaram no landing'
TBLPROPERTIES (
    'quality' = 'bronze',
    'pipelines.reset.allowed' = 'false'
)
AS SELECT 
    *,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '${landing_path}/reviews',
    format => 'json',
    schema => 'review_id STRING, order_id STRING, review_score STRING, review_comment_title STRING, review_comment_message STRING, review_creation_date STRING, review_answer_timestamp STRING'
);

CREATE OR REFRESH STREAMING TABLE olist_silver.reviews (
    CONSTRAINT order_id_presente EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT nota_entre_1_e_5 EXPECT (review_score BETWEEN 1 AND 5)
)
COMMENT 'Avaliações tipadas'
TBLPROPERTIES (
    'quality' = 'silver'
)
AS SELECT
    review_id,
    order_id,
    CAST(review_score AS INT) AS review_score,
    review_comment_title,
    review_comment_message,
    to_timestamp(review_creation_date) AS review_creation_date,
    to_timestamp(review_answer_timestamp) AS review_answer_timestamp,
    _ingested_at
FROM STREAM (olist_bronze.reviews)