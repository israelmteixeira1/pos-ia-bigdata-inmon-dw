{{ config(materialized='table', schema='presentation') }}

with sales_base as (
    select
        o.order_id,
        o.order_date,
        o.customer_id,
        oi.product_business_id,
        oi.source_system,
        oi.quantity,
        oi.unit_price,
        oi.discount,
        round(oi.line_amount, 2) as total_amount
    from {{ ref('orders') }} o
    join {{ ref('order_items') }} oi
      on o.order_business_id = oi.order_business_id
    where o.order_date is not null
),

sales_with_product as (
    select
        sb.order_id              as sale_id,
        sb.order_date,
        sb.customer_id,
        p.product_id,            -- surrogate key unificada
        sb.quantity,
        sb.unit_price,
        sb.discount,
        sb.total_amount
    from sales_base sb
    left join {{ ref('products') }} p
      on sb.product_business_id = p.product_business_id
     and sb.source_system       = p.source_system
)

select
    sale_id,
    order_date,
    customer_id,
    product_id,
    quantity,
    unit_price,
    discount,
    total_amount
from sales_with_product
