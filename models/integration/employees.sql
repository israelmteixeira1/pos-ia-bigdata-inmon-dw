{{ config(materialized='table', schema='integration') }}

SELECT
    employeeid,
    lastname,
    firstname,
    title,
    titleofcourtesy,
    birthdate,
    hiredate,
    address,
    city,
    region,
    postalcode,
    country,
    homephone,
    extension,
    photo,
    notes,
    reportsto,
    -- Derivado: Idade do funcionário
    FLOOR(DATEDIFF(day, birthdate, CURRENT_DATE()) / 365.25) as age,
    -- Derivado: Anos de casa (tempo desde contratação)
    FLOOR(DATEDIFF(day, hiredate, CURRENT_DATE()) / 365.25) as years_with_company,
    CURRENT_TIMESTAMP() as edw_inserted_at,
    'northwind_staging' as source_system
FROM {{ source('staging', 'EMPLOYEES') }}
WHERE employeeid IS NOT NULL
