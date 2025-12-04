{{ config(
    materialized = 'table',
    schema       = 'INTEGRATION',
    alias        = 'PRODUCTS'
) }}

with nw_products as (
    select
        cast(p.productid as varchar)   as product_business_id,
        'NORTHWIND'                    as source_system,
        p.productname                  as product_name,
        cast(p.supplierid as varchar)  as supplier_business_id,
        cast(p.categoryid as varchar)  as category_business_id,
        p.quantityperunit              as quantity_per_unit,
        p.unitprice                    as unit_price,
        p.unitsinstock                 as units_in_stock,
        p.unitsonorder                 as units_on_order,
        p.reorderlevel                 as reorder_level,
        p.discontinued                 as discontinued
    from {{ source('staging', 'PRODUCTS') }} p
),

sample_products as (
    select
        cast(p.id as varchar)          as product_business_id,
        'SAMPLE'                       as source_system,
        p.title                        as product_name,
        cast(null as varchar)          as supplier_business_id,
        p.category                     as category_business_id,
        cast(null as varchar)          as quantity_per_unit,
        p.price                        as unit_price,
        uniform(0, 40, random())       as units_in_stock,
        uniform(0, 40, random())       as units_on_order,
        uniform(0, 15, random())       as reorder_level,
        0                              as discontinued
    from {{ source('staging', 'SAMPLE_DB_PRODUCTS') }} p
),

union_all as (
    select * from nw_products
    union all
    select * from sample_products
),

-- Join com categories para pegar o category_id correto
with_category_id as (
    select
        u.product_business_id,
        u.source_system,
        u.product_name,
        u.supplier_business_id,
        coalesce(c.category_id, u.category_business_id) as category_business_id,
        u.quantity_per_unit,
        u.unit_price,
        u.units_in_stock,
        u.units_on_order,
        u.reorder_level,
        u.discontinued
    from union_all u
    left join {{ ref('categories') }} c
        on u.category_business_id = c.category_business_id
        and u.source_system = c.source_system
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
    current_timestamp()         as valid_from,
    cast(null as timestamp)     as valid_to,
    1                           as current_flag
from with_category_id
