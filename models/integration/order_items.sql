{{ config(materialized='table', schema='integration') }}

SELECT
    orderid,
    productid,
    unitprice,
    quantity,
    discount,
    -- Derivado: Valor bruto do item (antes de desconto)
    unitprice * quantity AS gross_amount,
    -- Derivado: Valor líquido do item (após desconto)
    unitprice * quantity * (1 - discount) AS net_amount,
    -- Derivado: Valor absoluto do desconto dado
    unitprice * quantity * discount AS discount_amount,
    -- Metadados
    CURRENT_TIMESTAMP() as edw_inserted_at,
    'northwind_staging' as source_system
FROM {{ source('staging', 'ORDERS_DETAILS') }}
WHERE orderid IS NOT NULL AND productid IS NOT NULL
