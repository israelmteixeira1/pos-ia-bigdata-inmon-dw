{{ config(materialized='table', schema='integration') }}

SELECT
    region_id,
    region_description,
    CURRENT_TIMESTAMP() as edw_inserted_at,
    'northwind_staging' as source_system
FROM {{ source('staging', 'regions') }}
WHERE region_id IS NOT NULL;
