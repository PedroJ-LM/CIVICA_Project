{{ config(materialized = 'table') }}

with s as (
  select
    store_id,
    store_name,
    zone_id,
    store_format_id,
    store_status_id,
    opening_time,
    closing_time,
    capacity_per_hour,
    floor_area_m2,
    gate_count,
    lat,
    lon,
    date_load_utc
  from {{ ref('stg_franchise_script__stores') }}
),

z as (
  select
    zone_id,
    zone_name,
    region_id,
    country_id
  from {{ ref('dim_zone') }}
),

fmt as (
  select
    store_format_id,
    store_format_name
  from {{ ref('stg_franchise_script__store_formats') }}
),

st as (
  select
    store_status_id,
    store_status_name
  from {{ ref('stg_franchise_script__store_status') }}
),

final as (
  select
    s.store_id,
    s.store_name,

    s.zone_id,
    z.zone_name,
    z.region_id,
    z.country_id,

    s.store_format_id,
    fmt.store_format_name,

    s.store_status_id,
    st.store_status_name,

    s.opening_time,
    s.closing_time,
    s.capacity_per_hour,
    s.floor_area_m2,
    s.gate_count,
    s.lat,
    s.lon,
    s.date_load_utc
  from s
  left join z
    on s.zone_id = z.zone_id
  left join fmt
    on s.store_format_id = fmt.store_format_id
  left join st
    on s.store_status_id = st.store_status_id
)

select *
from final
