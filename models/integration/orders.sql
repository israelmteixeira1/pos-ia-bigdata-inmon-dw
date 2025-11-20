{{ config(materialized='table', schema='integration') }}

SELECT
    orderid AS order_id,
    customerid AS customer_id,
    employeeid AS employee_id,
    orderdate AS order_date,
    requireddate AS required_date,
    shippeddate AS shipped_date,
    shipvia AS ship_via,
    freight,
    shipname AS ship_name,
    shipaddress AS ship_address,
    shipcity AS ship_city,
    shipregion AS ship_region,
    shippostalcode AS ship_postal_code,
    shipcountry AS ship_country,
    -- Derivado: Dias para envio, útil para medir eficiência logística
    DATEDIFF(day, orderdate, shippeddate) AS daystoship,
    -- Derivado: Status de entrega (no prazo ou não)
    CASE
        WHEN shippeddate IS NULL THEN 'PENDING'
        WHEN shippeddate <= requireddate THEN 'ON_TIME'
        ELSE 'LATE'
    END AS deliverystatus,
    -- Metadados
    CURRENT_TIMESTAMP() as edw_inserted_at,
    'northwind_staging' as source_system
FROM {{ source('staging', 'ORDERS') }}
WHERE orderid IS NOT NULL
