{{ config(materialized='table', schema='presentation') }}

SELECT
  employee_id,
  last_name,
  first_name,
  title,
  birth_date,
  hire_date,
  address,
  city,
  region,
  postal_code,
  country,
  home_phone
FROM {{ ref('employees') }}
