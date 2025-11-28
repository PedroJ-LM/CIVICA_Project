{{ config(materialized='table') }}

with zones_base as (
    select
        z.zone_id,
        z.zone_name,
        z.lat                     as zone_lat,
        z.lon                     as zone_lon,
        z.population,
        z.income_index,
        z.unemployment_rate_pct,

        -- claves de región y país ya calculadas en stg_zones
        z.region_id,
        r.region_name,

        z.country_id,
        c.country_name
    from {{ ref('stg_franchise_script__zones') }} z
    left join {{ ref('stg_franchise_script__regions') }} r
      on z.region_id = r.region_id
    left join {{ ref('stg_franchise_script__countries') }} c
      on z.country_id = c.country_id
),

country_currency as (
    select
        country_id,
        currency_code,
        currency_name
    from {{ ref('stg_franchise_script__countries_currency') }}
),

final as (
    select
        zb.zone_id,                -- PK de la dimensión
        zb.zone_name,
        zb.zone_lat,
        zb.zone_lon,
        zb.population,
        zb.income_index,
        zb.unemployment_rate_pct,
        zb.region_id,
        zb.region_name,
        zb.country_id,
        zb.country_name,
        cc.currency_code,
        cc.currency_name
    from zones_base zb
    left join country_currency cc
      on zb.country_id = cc.country_id
)

select *
from final