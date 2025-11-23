with src as (
  select * from {{ source('franchise_script', 'ZONE_WEATHER_DAYS') }}
),
typed as (
  select
    cast(zone_id as number)       as zone_id,
    cast(day as date)             as day,
    cast(temp_c as float)         as temp_c,
    cast(rainfall_mm as float)    as rainfall_mm,
    cast(wind_kmh as float)       as wind_kmh,
    cast(weather_label as string) as weather_label,
    CONVERT_TIMEZONE('UTC', _fivetran_synced) as date_load_utc,
    cast(_fivetran_deleted as string)         as _fivetran_deleted
  from src
)
select * from typed

