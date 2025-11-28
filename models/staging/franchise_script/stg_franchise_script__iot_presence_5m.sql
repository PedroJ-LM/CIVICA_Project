{{ config(
    materialized='incremental',
    incremental_strategy = 'append',
    unique_key=['store_id', 'ts_5m'],
    on_schema_change='sync_all_columns'
) }}

-- Grain: 1 row = (store_id, ts)

with source as (
    select *
    from {{ source('franchise_script', 'IOT_PRESENCE_5M') }}
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
        cast(store_id as number)    as store_id,
        cast(ts       as timestamp) as ts_5m,
        cast(devices  as number)    as devices,
        cast(mean_rssi as float)    as rssi_mean,
        cast(ts as date)            as day,
        cast(
          (extract(hour   from ts)*60
           + extract(minute from ts)) / 5
          as number
        ) as minute_5m_index,
        convert_timezone('UTC', cast(_fivetran_synced as timestamp_tz)) as date_load_utc
    from source
)

select * from renamed
