{{ config(materialized = 'view') }}

with base as (
    select
        zone_id,
        day,
        t_avg_c,
        rain_mm,
        wind_kmh,
        weather_label_id,
        date_load_utc
    from {{ ref('stg_franchise_script__zone_weather_days') }}
),

derived as (
    select
        b.zone_id,
        b.day,
        b.t_avg_c,
        b.rain_mm,
        b.wind_kmh,
        b.weather_label_id,
        b.date_load_utc,

        case
            when b.rain_mm >= 5 then 'HEAVY_RAIN'
            when b.rain_mm between 1 and 5 then 'LIGHT_RAIN'
            when b.t_avg_c >= 30 then 'HOT'
            when b.t_avg_c <= 5 then 'COLD'
            else 'MILD'
        end as weather_bucket,

        case when b.rain_mm >= 1 then 1 else 0 end as is_rainy,
        case when b.t_avg_c >= 28 then 1 else 0 end as is_hot,
        case when b.t_avg_c <= 5 then 1 else 0 end as is_cold
    from base b
)

select *
from derived
