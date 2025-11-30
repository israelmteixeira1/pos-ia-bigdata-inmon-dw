{{ config(materialized='table', schema='presentation') }}

select
  product_id,
  product_name,
  supplier_business_id as supplier_id,
  category_business_id as category_id,
  quantity_per_unit,
  unit_price,
  units_in_stock,
  units_on_order,
  reorder_level,
  discontinued
from {{ ref('products') }}
