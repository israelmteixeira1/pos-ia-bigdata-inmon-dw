{{ config(
    materialized='table',
    schema='integration'
) }}

SELECT
    CATEGORYID        AS category_id,
    CATEGORYNAME      AS category_name,
    DESCRIPTION       AS description,
    CURRENT_TIMESTAMP() AS edw_inserted_at,
    'northwind_staging' AS source_system
FROM {{ source('staging', 'CATEGORIES') }}
WHERE CATEGORYID IS NOT NULL
