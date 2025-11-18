{{ config(materialized='table', schema='integration') }}

SELECT
    employee_id,
    last_name,
    first_name,
    title,
    title_of_courtesy,
    birth_date,
    hire_date,
    address,
    city,
    region,
    postal_code,
    country,
    home_phone,
    extension,
    photo,
    notes,
    reports_to,
    -- Derivado: Idade do funcionário
    FLOOR(DATEDIFF(day, birth_date, CURRENT_DATE()) / 365.25) as age,
    -- Derivado: Anos de casa (tempo desde contratação)
    FLOOR(DATEDIFF(day, hire_date, CURRENT_DATE()) / 365.25) as years_with_company,
    CURRENT_TIMESTAMP() as edw_inserted_at,
    'northwind_staging' as source_system
FROM {{ source('staging', 'employees') }}
WHERE employee_id IS NOT NULL;
