CREATE OR REFRESH MATERIALIZED VIEW olist_gold.receita_diaria_uf
COMMENT 'Pedidos e receita por dia de compra e UF'
TBLPROPERTIES(
    'quality' = 'gold'
)
AS
WITH pagamentos AS (
    SELECT order_id, SUM(payment_value) AS valor_pago
    FROM olist_silver.payments
    GROUP BY order_id
)
SELECT
    DATE(o.purchase_ts) AS data_compra,
    c.customer_state AS uf,
    COUNT(*) AS pedidos,
    ROUND(SUM(p.valor_pago), 2) AS receita
FROM olist_silver.orders_current AS o
JOIN olist_silver.customers_current AS c
    ON o.customer_id = c.customer_id
LEFT JOIN pagamentos AS p
    ON o.order_id = p.order_id
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW olist_gold.desempenho_entrega_uf
COMMENT 'Pedidos entregues por UF: prazo médio e percentual com atraso'
TBLPROPERTIES(
    'quality' = 'gold'
)
AS SELECT
  c.customer_state AS uf,
  COUNT(*) AS pedidos_entregues,
  ROUND(AVG(datediff(o.delivered_ts, o.purchase_ts)), 1) AS dias_medios_entrega,
  ROUND(100 * AVG(CASE WHEN o.delivered_ts > o.estimated_delivery_ts THEN 1 ELSE 0 END), 1) AS pct_com_atraso
FROM olist_silver.orders_current AS o
JOIN olist_silver.customers_current AS c
  ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
  AND o.delivered_ts IS NOT NULL
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW olist_gold.avaliacao_por_categoria
COMMENT 'Nota média das avaliações por categoria de produto'
AS
WITH nota_pedido AS (
  SELECT order_id, AVG(review_score) AS nota
  FROM olist_silver.reviews
  GROUP BY order_id
)
SELECT
  i.product_category_en AS categoria,
  COUNT(DISTINCT i.order_id) AS pedidos_avaliados,
  ROUND(AVG(n.nota), 2) AS nota_media
FROM olist_silver.orders_items AS i
JOIN nota_pedido AS n
  ON i.order_id = n.order_id
GROUP BY ALL;