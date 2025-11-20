{{ config(materialized='table', schema='integration') }}

SELECT
    employeeid AS employee_id,
    lastname AS last_name,
    firstname AS first_name,
    title,
    titleofcourtesy,
    birthdate AS birth_date,
    hiredate AS hire_date,
    address,
    city,
    region,
    postalcode AS postal_code,
    country,
    homephone AS home_phone,
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
