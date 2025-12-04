{{ config(
    materialized='table',
    schema='presentation',
    alias='FCT_STOCK_MOVEMENTS'
) }}

with date_spine as (
    -- Gera 12 meses: janeiro a dezembro de 2025
    select 0 as month_offset union all
    select 1 union all
    select 2 union all
    select 3 union all
    select 4 union all
    select 5 union all
    select 6 union all
    select 7 union all
    select 8 union all
    select 9 union all
    select 10 union all
    select 11
),

snapshots as (
    select
        p.product_id,
        dateadd(month, -ds.month_offset, current_date()) as snapshot_date,
        case 
            when ds.month_offset = 0 then p.units_in_stock  -- dezembro 2025 = valor real
            else greatest(
                0, 
                p.units_in_stock + (uniform(-25, 25, random()) / 100.0 * p.units_in_stock)
            )::int
        end as quantity_in_stock,
        p.units_on_order as quantity_on_order,
        p.reorder_level,
        p.source_system
    from {{ ref('products') }} p
    cross join date_spine ds
    where p.units_in_stock is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['product_id', 'snapshot_date']) }} as movement_id,
    product_id,
    snapshot_date,
    quantity_in_stock,
    quantity_on_order,
    reorder_level,
    source_system
from snapshots
order by product_id, snapshot_date desc
