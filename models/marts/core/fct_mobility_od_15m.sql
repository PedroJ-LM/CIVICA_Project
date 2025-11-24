{{ config(materialized = 'table') }}

with mob as (
  select
    origin_zone_id,
    dest_store_id,
    ts_15m,
    arrivals,
    minute_15m_index,
    date_load_utc
  from {{ ref('stg_franchise_script__mobility_od_15m') }}
),

z as (
  select
    zone_id,
    lat,
    lon,
    population,
    income_index
  from {{ ref('dim_zone') }}
),

s as (
  select
    store_id,
    lat,
    lon,
    floor_area_m2
  from {{ ref('dim_store') }}
),

joined as (
  select
    m.origin_zone_id,
    m.dest_store_id,
    m.ts_15m,
    m.minute_15m_index,
    m.arrivals,
    m.date_load_utc,

    z.population,
    z.income_index,
    s.floor_area_m2,

    {{ haversine_km('z.lat', 'z.lon', 's.lat', 's.lon') }} as distance_km
  from mob m
  join z
    on m.origin_zone_id = z.zone_id
  join s
    on m.dest_store_id = s.store_id
),

final as (
  select
    origin_zone_id,
    dest_store_id,
    cast(ts_15m as date) as date_id,
    ts_15m,
    minute_15m_index,
    arrivals,
    distance_km,
    {{ gravity_potential(
         'population',
         'income_index',
         'floor_area_m2',
         'distance_km',
         alpha=1.0,
         beta=1.0,
         gamma=2.0
       ) }} as gravity_score,
    date_load_utc
  from joined
)

select *
from final
