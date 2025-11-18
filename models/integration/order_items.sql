{{ config(materialized='table', schema='integration') }}

SELECT
    order_id,
    product_id,
    unit_price,
    quantity,
    discount,
    -- Derivado: Valor bruto do item (antes de desconto)
    unit_price * quantity AS gross_amount,
    -- Derivado: Valor líquido do item (após desconto)
    unit_price * quantity * (1 - discount) AS net_amount,
    -- Derivado: Valor absoluto do desconto dado
    unit_price * quantity * discount AS discount_amount,
    -- Metadados
    CURRENT_TIMESTAMP() as edw_inserted_at,
    'northwind_staging' as source_system
FROM {{ source('staging', 'order_details') }}
WHERE order_id IS NOT NULL AND product_id IS NOT NULL;
