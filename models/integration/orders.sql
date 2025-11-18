{{ config(materialized='table', schema='integration') }}

SELECT
    order_id,
    customer_id,
    employee_id,
    order_date,
    required_date,
    shipped_date,
    ship_via AS shipper_id,
    freight,
    ship_name,
    ship_address,
    ship_city,
    ship_region,
    ship_postal_code,
    ship_country,
    -- Derivado: Dias para envio, útil para medir eficiência logística
    DATEDIFF(day, order_date, shipped_date) AS days_to_ship,
    -- Derivado: Status de entrega (no prazo ou não)
    CASE
        WHEN shipped_date IS NULL THEN 'PENDING'
        WHEN shipped_date <= required_date THEN 'ON_TIME'
        ELSE 'LATE'
    END AS delivery_status,
    -- Metadados
    CURRENT_TIMESTAMP() as edw_inserted_at,
    'northwind_staging' as source_system
FROM {{ source('staging', 'orders') }}
WHERE order_id IS NOT NULL;
