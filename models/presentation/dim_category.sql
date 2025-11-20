{{ config(materialized='table', schema='presentation') }}

SELECT
  category_id,
  category_name,
  description
FROM {{ ref('categories') }}
