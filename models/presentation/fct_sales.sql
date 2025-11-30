{{ config(materialized='table', schema='presentation') }}

select
  o.order_id                            as sale_id,
  o.order_date,
  o.customer_business_id                as customer_id,
  oi.product_business_id                as product_id,
  oi.quantity,
  oi.unit_price,
  oi.discount,
  round(oi.quantity * oi.unit_price * (1 - oi.discount), 2) as total_amount
from {{ ref('orders') }} o
join {{ ref('order_items') }} oi
  on o.order_business_id = oi.order_business_id
where o.order_date is not null
