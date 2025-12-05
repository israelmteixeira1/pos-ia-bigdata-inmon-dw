{{ config(
    materialized = "table",
    schema = "PRESENTATION",
    alias = "DIM_EXCHANGE_RATE"
) }}

select
    date                              as date_day,
    'USD'                             as currency_code,
    usd_rate                          as rate_to_usd,
    is_mock
from {{ ref('exchange_rate_usd') }} 
order by date_day
