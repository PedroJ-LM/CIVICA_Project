-- Grain: 1 row = (store_id, window_5m)
{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'merge',
    unique_key           = ['store_id','ts_5m'],
    on_schema_change     = 'sync_all_columns'
) }}

with source as (
    select * from {{ source('franchise_script', 'QUEUES_5M') }}
    {% if is_incremental() %}
      where date_load_utc > (select max(date_load_utc) from {{ this }})
    {% endif %}
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
