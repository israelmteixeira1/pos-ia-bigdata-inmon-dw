{{ config(materialized='table', schema='presentation') }}

select
  order_id,
  customer_id,
  employee_business_id as employee_id,
  order_date,
  required_date,
  shipped_date,
  shipper_business_id   as ship_via,
  freight,
  ship_name,
  ship_address,
  ship_city,
  ship_region,
  ship_postal_code,
  ship_country
from {{ ref('orders') }}
