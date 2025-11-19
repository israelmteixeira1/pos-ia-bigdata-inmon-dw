{{ config(materialized='table', schema='integration') }}

SELECT
    productid,
    productname,
    supplierid,
    categoryid,
    quantityperunit,
    unitprice,
    unitsinstock,
    unitsonorder,
    reorderlevel,
    discontinued,
    -- Derivado: Status do estoque do produto
    CASE 
        WHEN unitsinstock = 0 THEN 'OUT_OF_STOCK'
        WHEN unitsinstock <= reorderlevel THEN 'LOW_STOCK'
        ELSE 'OK'
    END as inventory_status,
    -- Derivado: Total disponível (estoque + em pedido)
    COALESCE(unitsinstock, 0) + COALESCE(unitsonorder, 0) as total_available,
    -- Metadados
    CURRENT_TIMESTAMP() as edw_inserted_at,
    'northwind_staging' as source_system
FROM {{ source('staging', 'PRODUCTS') }}
WHERE productid IS NOT NULL
