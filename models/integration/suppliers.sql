{{ config(materialized='table', schema='integration') }}

SELECT
    supplier_id,
    company_name,
    contact_name,
    contact_title,
    address,
    city,
    region,
    postal_code,
    country,
    phone,
    fax,
    homepage,
    -- Derivado: Agrupamento por região (potencial para análise geográfica de fornecedores)
    CASE 
        WHEN country IN ('USA', 'Canada', 'Mexico') THEN 'NORTHAM'
        WHEN country IN ('Germany', 'UK', 'France', 'Spain', 'Italy', 'Switzerland', 'Austria', 'Belgium', 'Sweden', 'Norway', 'Denmark', 'Poland', 'Ireland') THEN 'EUROPE'
        WHEN country IN ('Brazil', 'Argentina', 'Venezuela') THEN 'LATAM'
        ELSE 'OTHER'
    END as region_group,
    CURRENT_TIMESTAMP() as edw_inserted_at,
    'northwind_staging' as source_system
FROM {{ source('staging', 'suppliers') }}
WHERE supplier_id IS NOT NULL;
