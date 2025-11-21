-- Grain: 1 row = (store_id, ts)
-- Staging: cast types, derive day/minute_5m_index. No joins/agg.

with source as (
    select * from {{ source('franchise_script', 'IOT_PRESENCE_5M') }}
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

