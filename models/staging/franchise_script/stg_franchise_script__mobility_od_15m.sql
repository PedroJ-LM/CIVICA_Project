-- Grain: 1 row = (origin_zone_id, dest_store_id, ts_15m)
{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'merge',
    unique_key           = ['origin_zone_id','dest_store_id','ts_15m'],
    on_schema_change     = 'sync_all_columns'
) }}

with source as (
    select * from {{ source('franchise_script', 'MOBILITY_OD_15M') }}
    {% if is_incremental() %}
      where date_load_utc > (select max(date_load_utc) from {{ this }})
    {% endif %}
),
renamed as (
    select
        cast(ts as timestamp)               as ts_15m,
        cast(origin_zone_id as number)      as origin_zone_id,
        cast(dest_store_id as number)       as dest_store_id,
        cast(arrivals as number)            as arrivals,
        cast(ts as date)                    as day,
        cast((extract(hour from ts)*60 + extract(minute from ts)) / 15 as number) as minute_15m_index,
        CONVERT_TIMEZONE('UTC', CAST(_FIVETRAN_SYNCED AS TIMESTAMP_TZ)) AS date_load_utc
    from source
)
select * from renamed
