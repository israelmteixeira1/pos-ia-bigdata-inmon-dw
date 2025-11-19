{{ config(materialized='table', schema='integration') }}

SELECT
    shipperid,
    companyname,
    phone,
    CURRENT_TIMESTAMP() as edw_inserted_at,
    'northwind_staging' as source_system
FROM {{ source('staging', 'SHIPPERS') }}
WHERE shipperid IS NOT NULL
