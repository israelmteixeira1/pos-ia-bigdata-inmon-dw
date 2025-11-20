{{ config(materialized='table', schema='presentation') }}

SELECT
  o.order_id AS sale_id,
  o.order_date,
  o.customer_id,
  oi.product_id,
  oi.quantity,
  oi.unit_price,
  oi.discount,
  ROUND(oi.quantity * oi.unit_price * (1 - oi.discount), 2) AS total_amount
FROM {{ ref('orders') }} o
JOIN {{ ref('order_items') }} oi
  ON o.order_id = oi.order_id
WHERE o.order_date IS NOT NULL
