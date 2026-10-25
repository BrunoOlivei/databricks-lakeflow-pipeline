CREATE OR REFRESH MATERIALIZED VIEW olist_silver.dim_products
COMMENT 'Produtos com a categoria traduzida para inglês'
AS SELECT
  product_id,
  product_category_name,
  coalesce(product_category_name_english, product_category_name, 'sem_categoria') AS product_category_en,
  CAST(product_weight_g AS INT) AS product_weight_g,
  CAST(product_photos_qty AS INT) AS product_photos_qty
FROM read_files(
  '${landing_path}/dims/products/',
  format => 'json',
  schema => 'product_id STRING, product_category_name STRING, product_name_lenght STRING, product_description_lenght STRING, product_photos_qty STRING, product_weight_g STRING, product_length_cm STRING, product_height_cm STRING, product_width_cm STRING, product_category_name_english STRING'
);
 
CREATE OR REFRESH MATERIALIZED VIEW olist_silver.dim_sellers
COMMENT 'Vendedores'
AS SELECT
  seller_id,
  seller_zip_code_prefix,
  initcap(seller_city) AS seller_city,
  upper(seller_state) AS seller_state
FROM read_files(
  '${landing_path}/dims/sellers/',
  format => 'json',
  schema => 'seller_id STRING, seller_zip_code_prefix STRING, seller_city STRING, seller_state STRING'
);
 