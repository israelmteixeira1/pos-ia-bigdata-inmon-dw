{{ config(materialized='table', schema='integration') }}

SELECT
    category_id,
    category_name,
    description,
    CURRENT_TIMESTAMP() as edw_inserted_at,
    'northwind_staging' as source_system
FROM {{ source('staging', 'categories') }}
WHERE category_id IS NOT NULL;
