-- Grain: 1 row = (store_id, product_id, day)
-- Staging: tipado y nombres canónicos. Sin joins/agg.

with source as (
    select * from {{ source('franchise_script', 'INVENTORY_DAYS') }}
),
renamed as (
    select
        cast(store_id as number)          as store_id,
        cast(product_id as number)        as product_id,
        cast(day as date)                 as day,
        cast(stock as number)             as stock,
        cast(units_sold as number)        as units_sold,
        cast(units_replenished as number) as units_replenished,
        CONVERT_TIMEZONE('UTC', CAST(_FIVETRAN_SYNCED AS TIMESTAMP_TZ)) AS date_load_utc
    from source
)
select * from renamed

