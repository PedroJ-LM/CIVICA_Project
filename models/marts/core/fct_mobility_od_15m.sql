{{ config(
    materialized='incremental',
    unique_key=['origin_zone_id', 'dest_store_id', 'ts_15m'],
    on_schema_change='sync_all_columns'
) }}

with base as (
  select
    origin_zone_id,
    dest_store_id,
    ts_15m,
    arrivals,
    minute_15m_index,
    date_load_utc
  from {{ ref('stg_franchise_script__mobility_od_15m') }}
  {% if is_incremental() %}
    where cast(ts_15m as date) >
      (
        select coalesce(max(date_id), cast('1900-01-01' as date))
        from {{ this }}
      )
  {% endif %}
),

origin_zone as (
  select
    zone_id,
    lat           as origin_lat,
    lon           as origin_lon,
    population    as origin_population
  from {{ ref('dim_zone') }}
),

dest_store as (
  select
    store_id,
    lat                    as dest_lat,
    lon                    as dest_lon,
    floor_area_m2          as dest_floor_area_m2,
    capacity_per_hour      as dest_capacity_per_hour
  from {{ ref('dim_store') }}
),

with_geo as (
  select
    b.origin_zone_id,
    b.dest_store_id,
    b.ts_15m,
    b.minute_15m_index,
    b.arrivals,
    cast(b.ts_15m as date)              as date_id,

    oz.origin_population,
    ds.dest_floor_area_m2,
    ds.dest_capacity_per_hour,

    {{ distance_haversine_km(
         'oz.origin_lat',
         'oz.origin_lon',
         'ds.dest_lat',
         'ds.dest_lon'
       ) }}                            as distance_km
  from base b
  join origin_zone oz
    on b.origin_zone_id = oz.zone_id
  join dest_store ds
    on b.dest_store_id = ds.store_id
),

final as (
  select
    origin_zone_id,
    dest_store_id,
    date_id,
    ts_15m,
    minute_15m_index,
    arrivals,
    distance_km,
    {{ gravity_score('arrivals', 'distance_km') }} as gravity_score

  from with_geo
)

select * from final
