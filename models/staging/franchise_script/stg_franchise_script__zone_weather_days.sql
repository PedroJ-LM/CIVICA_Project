-- Grano: 1 fila = (zone_id, day). Staging sin joins/agg.
with source as (
    select * from {{ source('franchise_script', 'ZONE_WEATHER_DAYS') }}
),
renamed as (
    select
        cast(zone_id as number)              as zone_id,      -- ZONE_ID (NUMBER)
        cast(day as date)                    as day,          -- DAY (DATE)
        cast(temp_c as float)                as t_avg_c,      -- TEMP_C → canónico
        cast(rainfall_mm as float)           as rain_mm,      -- RAINFALL_MM → canónico
        cast(wind_kmh as float)              as wind_kmh,     -- WIND_KMH
        upper(cast(weather_label as string)) as weather_label -- WEATHER_LABEL
    from source
)
select * from renamed
