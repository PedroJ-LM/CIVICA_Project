-- Grain: 1 row = (store_id, gate_id, window_5m, direction)
-- Staging: cast types, normalize enums, derive day/minute_5m_index. No joins/agg.

with source as (
    select * from {{ source('franchise_script', 'ENTRIES_5M') }}
),
renamed as (
    select
        cast(store_id as number)            as store_id,
        cast(gate_id as number)             as gate_id,
        cast(window_5m as timestamp)        as ts_5m,
        upper(cast(direction as string))    as direction,       -- IN / OUT
        cast(entry_count as number)         as people_count,    -- ← Mapea desde ENTRY_COUNT
        upper(cast(technology as string))   as technology,
        cast(window_5m as date)             as day,
        cast((extract(hour from window_5m)*60 + extract(minute from window_5m)) / 5 as number) as minute_5m_index,
        CONVERT_TIMEZONE('UTC', CAST(_FIVETRAN_SYNCED AS TIMESTAMP_TZ)) AS date_load_utc
    from source
)
select * from renamed

