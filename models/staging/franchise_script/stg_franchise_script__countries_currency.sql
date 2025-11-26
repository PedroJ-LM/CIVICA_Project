{{ config(materialized='view') }}

with src as (
    select
        upper(trim(country_name))    as country_name,
        upper(trim(currency_code))   as currency_code,
        initcap(trim(currency_name)) as currency_name
    from {{ ref('franchise_script__countries_currency') }}  
)

select
    md5(country_name)       as country_id,
    country_name     as country_name,
    currency_code,
    currency_name
from src
