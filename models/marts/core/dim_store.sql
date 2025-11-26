-- models/marts/dim_store.sql
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

final as (
    select
        store_id,
        store_name,
        zone_id,              -- FK -> dim_zone.zone_id

        -- atributos propios de tienda
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
    from base_store
)

select *
from final
