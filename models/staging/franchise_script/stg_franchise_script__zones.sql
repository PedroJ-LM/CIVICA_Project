-- Grano: 1 fila = 1 zona del sistema origen.
-- Objetivo: tipar y normalizar nombres; sin joins/agg en staging.

with source as (
    select * from {{ source('franchise_script', 'ZONES') }}
),
renamed as (
    select
        cast(zone_id as number)           as zone_id,
        zone_name                          as zone_name,
        cast(lat as float)                as lat,
        cast(lon as float)                as lon,
        cast(population as number)        as population,
        cast(income_index as float)       as income_index,

        -- Mantener en porcentaje 0–100 y renombrar con sufijo _pct
        try_to_double(unemployment_rate)  as unemployment_rate_pct,
        CONVERT_TIMEZONE('UTC', CAST(_FIVETRAN_SYNCED AS TIMESTAMP_TZ)) AS date_load_utc
    from source
)
select * from renamed

