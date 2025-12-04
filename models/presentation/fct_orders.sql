{{ config(materialized='table', schema='presentation') }}

with orders_base as (
  select
    order_id,
    customer_id,
    employee_business_id as employee_id,
    order_date,
    required_date,
    shipped_date,
    shipper_business_id as ship_via,
    freight
  from {{ ref('orders') }}
),

mock_addresses as (
  select 
    order_id,
    -- Usa HASH para converter string em número
    mod(abs(hash(order_id)), 5) as address_variant,
    
    case mod(abs(hash(order_id)), 5)
      when 0 then '1600 Pennsylvania Ave NW'
      when 1 then '350 Fifth Avenue'
      when 2 then '221B Baker Street'
      when 3 then '742 Evergreen Terrace'
      else '1060 West Addison Street'
    end as ship_address,
    
    case mod(abs(hash(order_id)), 5)
      when 0 then 'Washington'
      when 1 then 'New York'
      when 2 then 'London'
      when 3 then 'Springfield'
      else 'Chicago'
    end as ship_city,
    
    case mod(abs(hash(order_id)), 5)
      when 0 then 'DC'
      when 1 then 'NY'
      when 2 then 'England'
      when 3 then 'IL'
      else 'IL'
    end as ship_region,
    
    case mod(abs(hash(order_id)), 5)
      when 0 then '20500'
      when 1 then '10118'
      when 2 then 'NW1 6XE'
      when 3 then '62701'
      else '60613'
    end as ship_postal_code,
    
    case mod(abs(hash(order_id)), 5)
      when 0 then 'USA'
      when 1 then 'USA'
      when 2 then 'UK'
      when 3 then 'USA'
      else 'USA'
    end as ship_country
  from {{ ref('orders') }}
)

select
  o.*,
  a.ship_address,
  a.ship_city,
  a.ship_region,
  a.ship_postal_code,
  a.ship_country,
  concat(
    a.ship_address, ', ',
    a.ship_city, ' ',
    a.ship_region, ' ',
    a.ship_postal_code
  ) as ship_name
from orders_base o
left join mock_addresses a using (order_id)
