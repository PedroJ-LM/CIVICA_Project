{{ config(
    materialized='incremental',
    incremental_strategy = 'merge',
    unique_key=['store_id', 'product_id', 'day'],
    on_schema_change='sync_all_columns'
) }}

-- Grain: 1 row = (store_id, product_id, day)

with source as (
    select *
    from {{ source('franchise_script', 'INVENTORY_DAYS') }}
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
        cast(store_id      as number) as store_id,
        cast(product_id    as number) as product_id,
        cast(day           as date)   as day,
        cast(stock         as number) as stock,
        cast(units_sold    as number) as units_sold,
        cast(units_replenished as number) as units_replenished,
        convert_timezone('UTC', cast(_fivetran_synced as timestamp_tz)) as date_load_utc
    from source
)

select * from renamed
