{{ config(materialized='table', schema='integration') }}

SELECT
    regionid,
    regiondescription,
    CURRENT_TIMESTAMP() as edw_inserted_at,
    'northwind_staging' as source_system
FROM {{ source('staging', 'REGIONS') }}
WHERE regionid IS NOT NULL
