{{ config(
    materialized = 'table',
    schema       = 'INTEGRATION',
    alias        = 'CUSTOMERS'
) }}

with nw_customers as (
    select
        cast(CustomerID as varchar)  as customer_business_id,
        'NORTHWIND'                  as source_system,
        CompanyName                  as company_name,
        ContactName                  as contact_name,
        ContactTitle                 as contact_title,
        Address                      as address,
        City                         as city,
        Region                       as region,
        cast(PostalCode as varchar)  as postal_code,
        Country                      as country,
        Phone                        as phone,
        Fax                          as fax
    from {{ source('staging', 'CUSTOMERS') }}
),

sample_people as (
    select
        cast(ID as varchar)          as customer_business_id,
        'SAMPLE_DB'                  as source_system,
        null                         as company_name,
        NAME                         as contact_name,
        null                         as contact_title,
        ADDRESS                      as address,
        CITY                         as city,
        STATE                        as region,
        cast(ZIP as varchar)         as postal_code,
        null                         as country,
        EMAIL                        as phone,   -- ou null se preferir
        null                         as fax
    from {{ source('staging', 'SAMPLE_DB_PEOPLE') }}
),

union_all as (
    select * from nw_customers
    union all
    select * from sample_people
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_business_id','source_system']) }} as customer_id,
    customer_business_id,
    source_system,
    company_name,
    contact_name,
    contact_title,
    address,
    city,
    region,
    postal_code,
    country,
    phone,
    fax,
    current_timestamp()  as valid_from,
    cast(null as timestamp) as valid_to,
    1                    as current_flag
from union_all