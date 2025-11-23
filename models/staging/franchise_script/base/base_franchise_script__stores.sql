with src as (
  select * from {{ source('franchise_script', 'STORES') }}
),
typed as (
  select
    cast(store_id as number)          as store_id,
    cast(store_name as string)        as store_name,
    cast(zone_id as number)           as zone_id,
    cast(lat as float)                as lat,
    cast(lon as float)                as lon,
    cast(capacity_per_hour as number) as capacity_per_hour,
    cast(store_format as string)      as store_format,
    cast(floor_area_m2 as number)     as floor_area_m2,
    cast(gate_count as number)        as gate_count,
    cast(opening_time as time)        as opening_time,
    cast(closing_time as time)        as closing_time,
    cast(status as string)            as status,
    CONVERT_TIMEZONE('UTC', _fivetran_synced) as date_load_utc,
    cast(_fivetran_deleted as string)         as _fivetran_deleted
  from src
)
select * from typed
