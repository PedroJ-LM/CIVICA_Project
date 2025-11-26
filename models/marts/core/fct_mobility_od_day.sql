{{ config(
    materialized     = 'incremental',
    unique_key       = ['origin_zone_id', 'dest_store_id', 'date_id'],
    on_schema_change = 'sync_all_columns'
) }}

with base as (
    select
        origin_zone_id,
        dest_store_id,
        date_id,
        arrivals_day,
        date_load_utc
    from {{ ref('int_mobility_od_15m_aggregated_to_day') }}
    {% if is_incremental() %}
    where date_id > (
        select coalesce(max(date_id), cast('1900-01-01' as date))
        from {{ this }}
    )
    {% endif %}
),

origin_zone as (
    select
        zone_id,
        lat          as origin_lat,
        lon          as origin_lon,
        population   as origin_population
    from {{ ref('stg_franchise_script__zones') }}
),

dest_store as (
    select
        store_id,
        lat                     as dest_lat,
        lon                     as dest_lon,
        floor_area_m2           as dest_floor_area_m2,
        capacity_per_hour       as dest_capacity_per_hour,
        zone_id,
        zone_name,
        region_id,
        region_name,
        country_id,
        country_name,
        store_format_id,
        store_status_id
    from {{ ref('dim_store') }}
),

weather as (
    select
        zone_id,
        day as date_id,
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
        b.arrivals_day,
        b.date_load_utc,

        oz.origin_population,

        ds.dest_floor_area_m2,
        ds.dest_capacity_per_hour,

        ds.zone_id        as dest_zone_id,
        ds.zone_name      as dest_zone_name,
        ds.region_id      as dest_region_id,
        ds.region_name    as dest_region_name,
        ds.country_id     as dest_country_id,
        ds.country_name   as dest_country_name,
        ds.store_format_id,
        ds.store_status_id,

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
        g.*,
        w.weather_bucket,
        w.is_rainy,
        w.is_hot,
        w.is_cold
    from with_geo g
    left join weather w
      on g.origin_zone_id = w.zone_id
     and g.date_id        = w.date_id
),

final as (
    select
        origin_zone_id,
        dest_store_id,
        date_id,

        arrivals_day,
        distance_km,
        {{ gravity_score('arrivals_day', 'distance_km') }} as gravity_score_day,

        origin_population,
        dest_floor_area_m2,
        dest_capacity_per_hour,

        dest_zone_id,
        dest_zone_name,
        dest_region_id,
        dest_region_name,
        dest_country_id,
        dest_country_name,
        store_format_id,
        store_status_id,

        weather_bucket,
        is_rainy,
        is_hot,
        is_cold,

        date_load_utc
    from with_weather
)

select *
from final
