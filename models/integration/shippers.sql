{{ config(materialized='table', schema='integration') }}

SELECT
    shipper_id,
    company_name,
    phone,
    CURRENT_TIMESTAMP() as edw_inserted_at,
    'northwind_staging' as source_system
FROM {{ source('staging', 'shippers') }}
WHERE shipper_id IS NOT NULL;
