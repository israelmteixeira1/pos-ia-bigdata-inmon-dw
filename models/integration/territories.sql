{{ config(materialized='table', schema='integration') }}

SELECT
    territory_id,
    territory_description,
    region_id,
    CURRENT_TIMESTAMP() as edw_inserted_at,
    'northwind_staging' as source_system
FROM {{ source('staging', 'territories') }}
WHERE territory_id IS NOT NULL;
