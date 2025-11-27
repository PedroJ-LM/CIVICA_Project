{{ config(materialized='table') }}

with base_store as (
    select
        store_id,
        upper(trim(store_name)) as store_name,
        zone_id,
        lat,
        lon,
        capacity_per_hour,
        floor_area_m2,
        gate_count,
        opening_time,
        closing_time,
        store_format_id,
        store_status_id,
        date_load_utc
    from {{ ref('stg_franchise_script__stores') }}
),

store_formats as (
    select
        store_format_id,
        store_format_name
        -- date_load_utc / _fivetran_deleted 
    from {{ ref('stg_franchise_script__store_formats') }}
),

store_status as (
    select
        store_status_id,
        store_status_name
        -- date_load_utc / _fivetran_deleted 
    from {{ ref('stg_franchise_script__store_status') }}
),

final as (
    select
        s.store_id,
        s.store_name,
        s.zone_id,              -- FK -> dim_zone.zone_id

        -- atributos propios de tienda
        s.lat,
        s.lon,
        s.capacity_per_hour,
        s.floor_area_m2,
        s.gate_count,
        s.opening_time,
        s.closing_time,

        -- formato: id + nombre
        s.store_format_id,
        sf.store_format_name,

        -- estado: id + nombre
        s.store_status_id,
        ss.store_status_name,

        s.date_load_utc
    from base_store s
    left join store_formats sf
      on s.store_format_id = sf.store_format_id
    left join store_status ss
      on s.store_status_id = ss.store_status_id
)

select *
from final
