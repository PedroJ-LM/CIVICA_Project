{{ config(
    materialized='incremental',
    unique_key=['origin_zone_id', 'dest_store_id', 'date_id'],
    on_schema_change='sync_all_columns'
) }}

with base as (
    select
        origin_zone_id,
        dest_store_id,
        date_id,
        arrivals
    from {{ ref('int_mobility_od_15m_aggregated_to_day') }}
    {% if is_incremental() %}
      where date_id > (
        select coalesce(max(date_id), cast('1900-01-01' as date))
        from {{ this }}
      )
    {% endif %}
),

-- 🔁 AHORA USAMOS dim_zone COMO ORIGEN DE LA ZONA
origin_zone as (
    select
        zone_id,
        zone_lat        as origin_lat,
        zone_lon        as origin_lon,
        population      as origin_population
    from {{ ref('dim_zone') }}
),

dest_store as (
    select
        store_id,
        lat               as dest_lat,
        lon               as dest_lon,
        floor_area_m2     as dest_floor_area_m2,
        capacity_per_hour as dest_capacity_per_hour
    from {{ ref('dim_store') }}
),

weather as (
    select
        zone_id,
        day,
        weather_bucket,
        is_rainy,
        is_hot,
        is_cold
    from {{ ref('int_zones_weather_days_enriched') }}
),

with_geo as (
    select
        b.origin_zone_id,
        b.dest_store_id,
        b.date_id,
        b.arrivals,
        oz.origin_population,
        ds.dest_floor_area_m2,
        ds.dest_capacity_per_hour,
        {{ distance_haversine_km(
            'oz.origin_lat',
            'oz.origin_lon',
            'ds.dest_lat',
            'ds.dest_lon'
        ) }} as distance_km
    from base b
    join origin_zone oz
      on b.origin_zone_id = oz.zone_id
    join dest_store ds
      on b.dest_store_id = ds.store_id
),

with_weather as (
    select
        g.origin_zone_id,
        g.dest_store_id,
        g.date_id,
        g.arrivals,
        g.distance_km,
        g.origin_population,
        g.dest_floor_area_m2,
        g.dest_capacity_per_hour,
        w.weather_bucket,
        w.is_rainy,
        w.is_hot,
        w.is_cold
    from with_geo g
    left join weather w
      on  g.origin_zone_id = w.zone_id
      and g.date_id        = w.day
),

final as (
    select
        origin_zone_id,
        dest_store_id,
        date_id,
        arrivals,
        distance_km,
        {{ gravity_score('arrivals', 'distance_km') }} as gravity_score,
        origin_population,
        dest_floor_area_m2,
        dest_capacity_per_hour,
        weather_bucket,
        is_rainy,
        is_hot,
        is_cold
    from with_weather
)

select *
from final
