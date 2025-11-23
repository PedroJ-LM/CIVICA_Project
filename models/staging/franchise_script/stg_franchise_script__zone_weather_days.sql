with base as (
  select *
  from {{ ref('base_franchise_script__zone_weather_days') }}
),
final as (
  select
    zone_id,
    day,
    temp_c       as t_avg_c,
    rainfall_mm  as rain_mm,
    wind_kmh,
    md5(upper(trim(weather_label))) as weather_label_id,
    date_load_utc,
    _fivetran_deleted
  from base
)
select * from final


