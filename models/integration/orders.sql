{{ config(
    materialized = 'table',
    schema       = 'INTEGRATION',
    alias        = 'ORDERS'
) }}

with nw_orders as (
    select
        cast(o.OrderID as varchar)    as order_business_id,
        'NORTHWIND'                   as source_system,
        cast(o.CustomerID as varchar) as customer_business_id,
        cast(o.EmployeeID as varchar) as employee_business_id,
        cast(o.ShipVia as varchar)    as shipper_business_id,
        o.OrderDate::string           as order_date,
        o.RequiredDate::string        as required_date,
        o.ShippedDate::string         as shipped_date,
        o.Freight                     as freight,
        o.ShipName                    as ship_name,
        o.ShipAddress                 as ship_address,
        o.ShipCity                    as ship_city,
        o.ShipRegion                  as ship_region,
        o.ShipPostalCode              as ship_postal_code,
        o.ShipCountry                 as ship_country
    from {{ source('staging', 'ORDERS') }} o
),

sample_orders as (
    select
        cast(ID as varchar)           as order_business_id,
        'SAMPLE_DB'                   as source_system,
        cast(USER_ID as varchar)      as customer_business_id,
        null                          as employee_business_id,
        null                          as shipper_business_id,
        CREATED_AT                    as order_date,
        CREATED_AT                    as required_date,
        CREATED_AT                    as shipped_date,
        TOTAL                         as freight,
        null                          as ship_name,
        null                          as ship_address,
        null                          as ship_city,
        null                          as ship_region,
        null                          as ship_postal_code,
        null                          as ship_country
    from {{ source('staging', 'SAMPLE_DB_ORDERS') }}
),

union_all as (
    select * from nw_orders
    union all
    select * from sample_orders
),

orders_enriched as (
    select
        ua.*,
        c.customer_id   -- ID artificial vindo de INTEGRATION.CUSTOMERS
    from union_all ua
    left join {{ ref('customers') }} c
      on ua.customer_business_id = c.customer_business_id
     and ua.source_system        = c.source_system
)

select
    {{ dbt_utils.generate_surrogate_key(['order_business_id','source_system']) }} as order_id,
    order_business_id,
    source_system,
    customer_id,          -- << AGORA USA O ID ARTIFICIAL
    employee_business_id,
    shipper_business_id,
    order_date,
    required_date,
    shipped_date,
    freight,
    ship_name,
    ship_address,
    ship_city,
    ship_region,
    ship_postal_code,
    ship_country,
    current_timestamp()       as valid_from,
    cast(null as timestamp)   as valid_to,
    1                         as current_flag
from orders_enriched
