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
    -- Usa HASH para converter string em número e distribui entre 0-9 (10 variações)
    mod(abs(hash(order_id)), 10) as address_variant,
    
    case mod(abs(hash(order_id)), 10)
      when 0 then '1600 Pennsylvania Ave NW'
      when 1 then '350 Fifth Avenue'
      when 2 then '221B Baker Street'
      when 3 then '742 Evergreen Terrace'
      when 4 then '1060 West Addison Street'
      when 5 then '10 Downing Street'
      when 6 then 'Champs-Élysées 75008'
      when 7 then 'Alexanderplatz 1'
      when 8 then 'Shibuya Crossing'
      else 'Rua Augusta 1000'
    end as ship_address,
    
    case mod(abs(hash(order_id)), 10)
      when 0 then 'Washington'
      when 1 then 'New York'
      when 2 then 'London'
      when 3 then 'Springfield'
      when 4 then 'Chicago'
      when 5 then 'London'
      when 6 then 'Paris'
      when 7 then 'Berlin'
      when 8 then 'Tokyo'
      else 'São Paulo'
    end as ship_city,
    
    case mod(abs(hash(order_id)), 10)
      when 0 then 'DC'
      when 1 then 'NY'
      when 2 then 'England'
      when 3 then 'IL'
      when 4 then 'IL'
      when 5 then 'England'
      when 6 then 'Île-de-France'
      when 7 then 'Berlin'
      when 8 then 'Tokyo'
      else 'SP'
    end as ship_region,
    
    case mod(abs(hash(order_id)), 10)
      when 0 then '20500'
      when 1 then '10118'
      when 2 then 'NW1 6XE'
      when 3 then '62701'
      when 4 then '60613'
      when 5 then 'SW1A 2AA'
      when 6 then '75008'
      when 7 then '10178'
      when 8 then '150-0002'
      else '01310-100'
    end as ship_postal_code,
    
    case mod(abs(hash(order_id)), 10)
      when 0 then 'USA'
      when 1 then 'USA'
      when 2 then 'UK'
      when 3 then 'USA'
      when 4 then 'USA'
      when 5 then 'UK'
      when 6 then 'France'
      when 7 then 'Germany'
      when 8 then 'Japan'
      else 'Brazil'
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
