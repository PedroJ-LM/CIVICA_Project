{{ config(
    materialized='incremental',
    incremental_strategy = 'append',
    unique_key=['store_id', 'gate_id', 'ts_5m', 'direction_id'],
    on_schema_change='sync_all_columns'
) }}

-- Grain: (store_id, gate_id, ts_5m, direction_id)

with source as (
    select *
    from {{ source('franchise_script', 'ENTRIES_5M') }}
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
        cast(store_id  as number)         as store_id,
        cast(gate_id   as number)         as gate_id,
        cast(window_5m as timestamp_ntz)  as ts_5m,

        upper(cast(direction as string))  as direction_name,
        cast(md5(upper(trim(direction))) as string) as direction_id,

        cast(entry_count as number)       as people_count,
        cast(technology as string)        as technology,

        cast(date_trunc('day', window_5m) as date) as day,
        cast(
          (date_part(hour,   window_5m)*60
           + date_part(minute, window_5m)) / 5
          as number
        ) as minute_5m_index,

        convert_timezone('UTC', _fivetran_synced) as date_load_utc
    from source
)

select * from renamed
