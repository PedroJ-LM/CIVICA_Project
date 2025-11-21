-- Grain: 1 row = (store_id, window_5m)
-- Staging: cast types, derive day/minute_5m_index. No joins/agg.

with source as (
    select * from {{ source('franchise_script', 'QUEUES_5M') }}
),
renamed as (
    select
        cast(store_id as number)            as store_id,
        cast(window_5m as timestamp)        as ts_5m,
        cast(avg_queue_len as number)       as queue_avg,      -- ← Mapea desde AVG_QUEUE_LEN
        cast(avg_wait_sec as number)        as wait_avg_s,     -- ← Mapea desde AVG_WAIT_SEC
        cast(window_5m as date)             as day,
        cast((extract(hour from window_5m)*60 + extract(minute from window_5m)) / 5 as number) as minute_5m_index,
        CONVERT_TIMEZONE('UTC', CAST(_FIVETRAN_SYNCED AS TIMESTAMP_TZ)) AS date_load_utc
    from source
)
select * from renamed
