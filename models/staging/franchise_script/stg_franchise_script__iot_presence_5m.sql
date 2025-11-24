-- Grain: 1 row = (store_id, ts)
{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'merge',
    unique_key           = ['store_id','ts_5m'],
    on_schema_change     = 'sync_all_columns'
) }}

with source as (
    select * from {{ source('franchise_script', 'IOT_PRESENCE_5M') }}
    {% if is_incremental() %}
      where date_load_utc > (select max(date_load_utc) from {{ this }})
    {% endif %}
),
renamed as (
    select
        cast(store_id as number)            as store_id,
        cast(ts as timestamp)               as ts_5m,
        cast(devices as number)             as devices,
        cast(mean_rssi as float)            as rssi_mean,      -- ← Mapea desde MEAN_RSSI
        cast(ts as date)                    as day,
        cast((extract(hour from ts)*60 + extract(minute from ts)) / 5 as number) as minute_5m_index,
        CONVERT_TIMEZONE('UTC', CAST(_FIVETRAN_SYNCED AS TIMESTAMP_TZ)) AS date_load_utc
    from source
)
select * from renamed

