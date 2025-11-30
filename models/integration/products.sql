{{ config(
    materialized = 'table',
    schema       = 'INTEGRATION',
    alias        = 'PRODUCTS'
) }}

with nw_products as (
    select
        cast(p.ProductID as varchar)   as product_business_id,
        'NORTHWIND'                    as source_system,
        p.ProductName                  as product_name,
        p.SupplierID                   as supplier_business_id,
        cast(p.CategoryID as varchar)  as category_business_id,
        p.QuantityPerUnit              as quantity_per_unit,
        p.UnitPrice                    as unit_price,
        p.UnitsInStock                 as units_in_stock,
        p.UnitsOnOrder                 as units_on_order,
        p.ReorderLevel                 as reorder_level,
        p.Discontinued                 as discontinued
    from {{ source('staging', 'PRODUCTS') }} p
),

sample_products as (
    select
        cast(ID as varchar)            as product_business_id,
        'SAMPLE_DB'                    as source_system,
        TITLE                          as product_name,
        null                           as supplier_business_id,
        CATEGORY                       as category_business_id,
        null                           as quantity_per_unit,
        PRICE                          as unit_price,
        null                           as units_in_stock,
        null                           as units_on_order,
        null                           as reorder_level,
        0                              as discontinued      -- não há flag de arquivado
    from {{ source('staging', 'SAMPLE_DB_PRODUCTS') }}
),

union_all as (
    select * from nw_products
    union all
    select * from sample_products
)

select
    {{ dbt_utils.generate_surrogate_key(['product_business_id','source_system']) }} as product_id,
    product_business_id,
    source_system,
    product_name,
    supplier_business_id,
    category_business_id,
    quantity_per_unit,
    unit_price,
    units_in_stock,
    units_on_order,
    reorder_level,
    discontinued,
    current_timestamp()  as valid_from,
    cast(null as timestamp) as valid_to,
    1                    as current_flag
from union_all
