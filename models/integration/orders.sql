{{ config(materialized='table', schema='integration') }}

SELECT
    orderid,
    customerid,
    employeeid,
    orderdate,
    requireddate,
    shippeddate,
    shipvia AS shipperid,
    freight,
    shipname,
    shipaddress,
    shipcity,
    shipregion,
    shippostalcode,
    shipcountry,
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
