{{ config(
    materialized = 'table',
    schema       = 'INTEGRATION',
    alias        = 'ORDER_ITEMS'
) }}

with nw_items as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['cast(od.OrderID as varchar)', 'cast(od.ProductID as varchar)', '\'NORTHWIND\'']
        ) }}                             as order_item_id,
        cast(od.OrderID as varchar)      as order_business_id,
        'NORTHWIND'                      as source_system,
        cast(od.ProductID as varchar)    as product_business_id,
        od.Quantity                      as quantity,
        od.UnitPrice                     as unit_price,
        od.Discount                      as discount
    from {{ source('staging', 'ORDERS_DETAILS') }} od
),

sample_items as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['cast(o.ID as varchar)', 'cast(o.PRODUCT_ID as varchar)', '\'SAMPLE_DB\'']
        ) }}                             as order_item_id,
        cast(o.ID as varchar)            as order_business_id,
        'SAMPLE_DB'                      as source_system,
        cast(o.PRODUCT_ID as varchar)    as product_business_id,
        o.QUANTITY                       as quantity,
        -- TOTAL é subtotal + imposto; aqui usamos SUBTOTAL como unit_price aproximado
        o.SUBTOTAL / nullif(o.QUANTITY, 0) as unit_price,
        CASE
            WHEN o.DISCOUNT > 1 THEN o.DISCOUNT / 100.0  -- 67 -> 0.67
            ELSE o.DISCOUNT
        END                              as discount
    from {{ source('staging', 'SAMPLE_DB_ORDERS') }} o
),

union_all as (
    select * from nw_items
    union all
    select * from sample_items
)

select
    order_item_id,
    order_business_id,
    source_system,
    product_business_id,
    quantity,
    unit_price,
    discount,
    quantity * unit_price * (1 - discount) as line_amount,
    current_timestamp()                    as loaded_at
from union_all
