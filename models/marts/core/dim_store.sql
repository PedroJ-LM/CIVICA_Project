{{ config(materialized='table') }}

with base_store as (
    select
        store_id,
        upper(trim(store_name)) as store_name,
        zone_id,
        lat,
        lon,
        capacity_per_hour,
        floor_area_m2,
        gate_count,
        opening_time,
        closing_time,
        store_format_id,
        store_status_id,
        date_load_utc
    from {{ ref('stg_franchise_script__stores') }}
),

zone_hierarchy as (
    -- Capa “limpia” de jerarquía zona→región→país
    select
        zh.zone_name_clean,   -- ya viene upper + trim desde base_
        zh.region_name,
        zh.country_name,
        r.region_id,
        r.region_name as region_name_std,
        c.country_id,
        c.country_name as country_name_std
    from {{ ref('base_franchise_script__zones_hierarchy') }} zh
    left join {{ ref('stg_franchise_script__regions') }} r
      on zh.region_name = r.region_name      -- ambas están upper + trim
    left join {{ ref('stg_franchise_script__countries') }} c
      on zh.country_name = c.country_name    -- idem
),

zones as (
    select
        z.zone_id,
        upper(trim(z.zone_name))        as zone_name,
        z.lat                           as zone_lat,
        z.lon                           as zone_lon,
        z.population,
        z.income_index,
        z.unemployment_rate_pct,

        h.region_id,
        coalesce(h.region_name_std, h.region_name)     as region_name,
        h.country_id,
        coalesce(h.country_name_std, h.country_name)   as country_name
    from {{ ref('stg_franchise_script__zones') }} z
    left join zone_hierarchy h
      on upper(trim(z.zone_name)) = h.zone_name_clean
),

final as (
    select
        s.store_id,
        s.store_name,
        s.zone_id,

        -- Geografía desnormalizada
        z.zone_name,
        z.zone_lat,
        z.zone_lon,
        z.population,
        z.income_index,
        z.unemployment_rate_pct,
        z.region_id,
        z.region_name,
        z.country_id,
        z.country_name,

        -- Atributos propios de tienda
        s.lat,
        s.lon,
        s.capacity_per_hour,
        s.floor_area_m2,
        s.gate_count,
        s.opening_time,
        s.closing_time,
        s.store_format_id,
        s.store_status_id,
        s.date_load_utc
    from base_store s
    left join zones z
      on s.zone_id = z.zone_id
)

select *
from final

