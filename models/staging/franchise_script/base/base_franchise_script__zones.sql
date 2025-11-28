-- Grano: 1 fila = 1 zona del sistema origen. 
-- Objetivo: tipar y normalizar nombres; sin joins/agg en BASE.

with source as (
    select *
    from {{ source('franchise_script', 'ZONES') }}
),

renamed as (
    select
        -- Clave natural
        cast(zone_id as number) as zone_id,

        -- Nombre de zona: UPPER y sin sufijo " #N"
        upper(
          regexp_replace(trim(zone_name), '\\s*#\\d+$', '')
        ) as zone_name,

        -- Nombres de región y país ya vienen del generador
        upper(trim(region_name))  as region_name,
        upper(trim(country_name)) as country_name,

        -- Geometría y atributos socioeconómicos
        cast(lat as float)         as lat,
        cast(lon as float)         as lon,
        cast(population as number) as population,
        cast(income_index as float) as income_index,

        -- Mantener en porcentaje 0–100 y renombrar con sufijo _pct
        try_to_double(unemployment_rate) as unemployment_rate_pct,

        -- Metadatos de carga (desde el loader tipo Fivetran-like)
        convert_timezone('UTC', cast(_FIVETRAN_SYNCED as timestamp_tz)) as date_load_utc,
        _fivetran_deleted
    from source
)

select *
from renamed
