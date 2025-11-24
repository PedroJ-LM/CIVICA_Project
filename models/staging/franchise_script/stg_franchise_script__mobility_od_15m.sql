{{ config(
    materialized='incremental',
    unique_key=['origin_zone_id', 'dest_store_id', 'ts_15m'],
    on_schema_change='sync_all_columns'
) }}

-- Grain: 1 row = (origin_zone_id, dest_store_id, ts_15m)

with source as (
    select *
    from {{ source('franchise_script', 'MOBILITY_OD_15M') }}
    {% if is_incremental() %}
      where convert_timezone('UTC', _fivetran_synced) > (
        select coalesce(
          max(date_load_utc),
          '1900-01-01'::timestamp_ntz
        )
        from {{ this }}
      )
    {% endif %}
),

renamed as (
    select
        cast(ts             as timestamp) as ts_15m,
        cast(origin_zone_id as number)    as origin_zone_id,
        cast(dest_store_id  as number)    as dest_store_id,
        cast(arrivals       as number)    as arrivals,
        cast(ts             as date)      as day,
        cast(
          (extract(hour   from ts)*60
           + extract(minute from ts)) / 15
          as number
        ) as minute_15m_index,
        convert_timezone('UTC', cast(_fivetran_synced as timestamp_tz)) as date_load_utc
    from source
)

select * from renamed
