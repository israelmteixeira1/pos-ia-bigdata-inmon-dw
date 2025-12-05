{{ config(
    materialized = "table",
    schema = "INTEGRATION",
    alias = "EXCHANGE_RATE_USD"
) }}

-- 1) Base: apenas DATE + USD vindos da staging
with base as (
    select
        "DATE"::date   as date_day,
        "USD"::float   as usd_rate_real
    from {{ source('staging', 'EXCHANGE_RATE') }}
),

-- 2) Números sequenciais para gerar datas
nums as (
    select
        seq4() as n
    from table(generator(rowcount => 12000))  -- constante literal
),

-- 3) Spine de datas de 1996-01-01 a 2026-12-31
date_spine as (
    select
        dateadd(day, n, to_date('1996-01-01')) as date_day
    from nums
    where dateadd(day, n, to_date('1996-01-01')) <= to_date('2026-12-31')
),

-- 4) Junta spine com as taxas reais de USD
joined as (
    select
        d.date_day,
        b.usd_rate_real
    from date_spine d
    left join base b
        on d.date_day = b.date_day
),

-- 5) Preenche datas sem taxa com valor mockado
final as (
    select
        date_day                                as date,
        coalesce(
            usd_rate_real,
            round(uniform(0.7::float, 1.1::float, random()), 4)
        ) as usd_rate,                          
        usd_rate_real is null                   as is_mock
    from joined
)

select *
from final
order by date
