{{ config(materialized='table', schema='presentation') }}

SELECT
  product_id,
  SUM(units_in_stock) AS total_stock,
  SUM(units_on_order) AS total_on_order,
  MIN(reorder_level) AS reorder_level
FROM {{ ref('products') }}
GROUP BY product_id
