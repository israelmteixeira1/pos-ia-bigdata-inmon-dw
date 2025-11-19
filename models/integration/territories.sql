{{ config(materialized='table', schema='integration') }}

SELECT
    territoryid,
    territorydescription,
    regionid,
    CURRENT_TIMESTAMP() as edw_inserted_at,
    'northwind_staging' as source_system
FROM {{ source('staging', 'TERRITORIES') }}
WHERE territoryid IS NOT NULL
