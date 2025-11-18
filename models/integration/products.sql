{{ config(materialized='table', schema='integration') }}

SELECT
    product_id,
    product_name,
    supplier_id,
    category_id,
    quantity_per_unit,
    unit_price,
    units_in_stock,
    units_on_order,
    reorder_level,
    discontinued,
    -- Derivado: Status do estoque do produto
    CASE 
        WHEN units_in_stock = 0 THEN 'OUT_OF_STOCK'
        WHEN units_in_stock <= reorder_level THEN 'LOW_STOCK'
        ELSE 'OK'
    END as inventory_status,
    -- Derivado: Total disponível (estoque + em pedido)
    COALESCE(units_in_stock, 0) + COALESCE(units_on_order, 0) as total_available,
    -- Metadados
    CURRENT_TIMESTAMP() as edw_inserted_at,
    'northwind_staging' as source_system
FROM {{ source('staging', 'products') }}
WHERE product_id IS NOT NULL;
