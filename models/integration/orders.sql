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
        to_date(o.OrderDate)          as order_date,
        to_date(o.RequiredDate)       as required_date,
        to_date(o.ShippedDate)        as shipped_date,
        o.Freight                     as freight,
        o.ShipName                    as ship_name,
        o.ShipAddress                 as ship_address,
        o.ShipCity                    as ship_city,
        o.ShipRegion                  as ship_region,
        o.ShipPostalCode              as ship_postal_code,
        o.ShipCountry                 as ship_country
    from {{ source('staging', 'ORDERS') }} o
),

sample_src as (
    select
        ID,
        USER_ID,
        TOTAL,
        lower(CREATED_AT) as created_lower
    from {{ source('staging', 'SAMPLE_DB_ORDERS') }}
),

sample_parsed as (
    select
        ID,
        USER_ID,
        TOTAL,
        split_part(created_lower, ' ', 1)  as mes_pt,    -- fevereiro
        split_part(created_lower, ' ', 2)  as dia_raw,   -- 11,
        split_part(created_lower, ' ', 3)  as ano_raw    -- 2025,
    from sample_src
),

sample_normalized as (
    select
        ID,
        USER_ID,
        TOTAL,
        case
            when mes_pt like 'janeiro%'  then '01'
            when mes_pt like 'fevereiro%' then '02'
            when mes_pt like 'mar%'      then '03'
            when mes_pt like 'abril%'    then '04'
            when mes_pt like 'maio%'     then '05'
            when mes_pt like 'junho%'    then '06'
            when mes_pt like 'julho%'    then '07'
            when mes_pt like 'agosto%'   then '08'
            when mes_pt like 'setembro%' then '09'
            when mes_pt like 'outubro%'  then '10'
            when mes_pt like 'novembro%' then '11'
            when mes_pt like 'dezembro%' then '12'
        end as mes_num,
        regexp_replace(dia_raw, '[^0-9]', '') as dia_num,
        regexp_replace(ano_raw, '[^0-9]', '') as ano_num
    from sample_parsed
),

sample_orders as (
    select
        cast(ID as varchar)           as order_business_id,
        'SAMPLE_DB'                   as source_system,
        cast(USER_ID as varchar)      as customer_business_id,
        null                          as employee_business_id,
        null                          as shipper_business_id,
        to_date(ano_num || '-' || mes_num || '-' || dia_num) as order_date,
        to_date(ano_num || '-' || mes_num || '-' || dia_num) as required_date,
        to_date(ano_num || '-' || mes_num || '-' || dia_num) as shipped_date,
        TOTAL                         as freight,
        null                          as ship_name,
        null                          as ship_address,
        null                          as ship_city,
        null                          as ship_region,
        null                          as ship_postal_code,
        null                          as ship_country
    from sample_normalized
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
    customer_id,
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
