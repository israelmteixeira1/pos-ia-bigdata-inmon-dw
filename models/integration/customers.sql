{{ config(materialized='table', schema='integration') }}

SELECT
    customerid         AS customer_id,
    companyname        AS company_name,
    contactname        AS contact_name,
    contacttitle       AS contact_title,
    address            AS address,
    city               AS city,
    region             AS region,
    postalcode         AS postal_code,
    country            AS country,
    phone              AS phone,
    fax                AS fax,
    CASE 
        WHEN country IN ('USA', 'Canada', 'Mexico') THEN 'NORTHAM'
        WHEN country IN ('Germany', 'UK', 'France', 'Spain', 'Italy', 'Switzerland', 'Austria', 'Belgium', 'Sweden', 'Norway', 'Denmark', 'Poland', 'Ireland') THEN 'EUROPE'
        WHEN country IN ('Brazil', 'Argentina', 'Venezuela') THEN 'LATAM'
        ELSE 'OTHER'
    END AS region_group,
    CURRENT_TIMESTAMP() AS edw_inserted_at,
    'northwind_staging' AS source_system
FROM {{ source('staging', 'CUSTOMERS') }}
WHERE customerid IS NOT NULL
