{{ config(materialized='view') }}

-- Grano: 1 fila = 1 zona del sistema origen.
-- Objetivo: añadir FKs a región y país usando el mismo id que en stg_regions / stg_countries.

with base as (
    select *
    from {{ ref('base_franchise_script__zones') }}
),

final as (
    select
        -- PK natural de la zona
        zone_id,
        zone_name,

        -- Geometría y atributos socioeconómicos
        lat,
        lon,
        population,
        income_index,
        unemployment_rate_pct,

        -- IDs compartidos con stg_regions y stg_countries
        to_varchar(md5(upper(trim(region_name))))  as region_id,
        to_varchar(md5(upper(trim(country_name)))) as country_id,

        -- Metadatos de carga
        date_load_utc,
        _fivetran_deleted
    from base
)

select *
from final
