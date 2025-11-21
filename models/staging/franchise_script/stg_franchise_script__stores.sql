with source as (
    select * from {{ source('franchise_script', 'STORES') }}
),
renamed as (
    select
        cast(store_id as number)               as store_id,
        store_name                              as store_name,
        cast(zone_id as number)                as zone_id,
        cast(lat as float)                     as lat,
        cast(lon as float)                     as lon,
        cast(capacity_per_hour as number)      as capacity_per_hour,
        upper(cast(store_format as string))    as store_format,
        cast(floor_area_m2 as number)          as floor_area_m2,
        cast(gate_count as number)             as gate_count,
        try_to_time(opening_time)              as opening_time,
        try_to_time(closing_time)              as closing_time,
        upper(cast(status as string))          as status,
        CONVERT_TIMEZONE('UTC', CAST(_FIVETRAN_SYNCED AS TIMESTAMP_TZ)) AS date_load_utc
    from source
)
select * from renamed
