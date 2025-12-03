{{ config(
    materialized='table',
    schema='integration'
) }}

-- Categorias do Northwind (já normalizadas)
with nw_categories as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['cast(categoryid as varchar)', '\'NORTHWIND\'']
        ) }}                        as category_id,
        cast(categoryid as varchar) as category_business_id,
        'NORTHWIND'                 as source_system,
        categoryname                as category_name,
        description                 as description,
        current_timestamp()         as edw_inserted_at
    from {{ source('staging', 'CATEGORIES') }}
    where categoryid is not null
),

-- Categorias do Sample (extrair valores únicos da coluna CATEGORY)
sample_categories as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(
            ['category', '\'SAMPLE\'']
        ) }}                 as category_id,
        category             as category_business_id,
        'SAMPLE'             as source_system,
        category             as category_name,
        cast(null as varchar) as description,
        current_timestamp()   as edw_inserted_at
    from {{ source('staging', 'SAMPLE_DB_PRODUCTS') }}
    where category is not null
),

union_all as (
    select * from nw_categories
    union all
    select * from sample_categories
)

select
    category_id,
    category_business_id,
    source_system,
    category_name,
    description,
    edw_inserted_at
from union_all
