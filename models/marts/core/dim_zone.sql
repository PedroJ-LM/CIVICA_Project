{{ config(materialized = 'table') }}

with z as (
  select
    zone_id,
    zone_name,
    lat,
    lon,
    population,
    income_index,
    unemployment_rate_pct,
    date_load_utc
  from {{ ref('stg_franchise_script__zones') }}
),

h as (
  select
    zone_name_clean,
    region_name,
    country_name
  from {{ ref('base_franchise_script__zones_hierarchy') }}
),

z_norm as (
  select
    z.*,
    translate(
      upper(trim(zone_name)),
      'ÁÉÍÓÚÜÑáéíóúüñ',
      'AEIOUUNAEIOUUN'
    ) as zone_name_norm
  from z
),

h_norm as (
  select
    h.*,
    translate(
      upper(trim(zone_name_clean)),
      'ÁÉÍÓÚÜÑáéíóúüñ',
      'AEIOUUNAEIOUUN'
    ) as zone_name_norm
  from h
),

z_with_names as (
  select
    z.zone_id,
    z.zone_name,
    z.lat,
    z.lon,
    z.population,
    z.income_index,
    z.unemployment_rate_pct,
    z.date_load_utc,
    h.region_name,
    h.country_name
  from z_norm z
  left join h_norm h
    on z.zone_name_norm = h.zone_name_norm
),

c as (
  select
    country_id,
    country_name
  from {{ ref('stg_franchise_script__countries') }}
),

r as (
  select
    region_id,
    region_name,
    country_id
  from {{ ref('stg_franchise_script__regions') }}
),

final as (
  select
    z.zone_id,
    z.zone_name,
    z.lat,
    z.lon,
    z.population,
    z.income_index,
    z.unemployment_rate_pct,

    r.region_id,
    r.region_name,

    c.country_id,
    c.country_name,

    z.date_load_utc
  from z_with_names z
  left join c
    on z.country_name = c.country_name
  left join r
    on z.region_name = r.region_name
   and c.country_id   = r.country_id
)

select *
from final
