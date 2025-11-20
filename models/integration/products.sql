{{ config(materialized='table', schema='integration') }}

SELECT
    productid AS product_id,
    productname AS product_name,
    supplierid AS supplier_id,
    categoryid AS category_id,
    quantityperunit AS quantity_per_unit,
    unitprice AS unit_price,
    unitsinstock AS units_in_stock,
    unitsonorder AS units_on_order,
    reorderlevel AS reorder_level,
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
