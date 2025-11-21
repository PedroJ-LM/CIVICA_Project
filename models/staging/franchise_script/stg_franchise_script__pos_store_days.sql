-- Grano: 1 fila = (store_id, day). Staging sin joins/agg.
with source as (
    select * from {{ source('franchise_script', 'POS_STORE_DAYS') }}
),
renamed as (
    select
        cast(day as date)                    as day,               -- DAY (DATE)
        cast(store_id as number)             as store_id,          -- STORE_ID (NUMBER)
        cast(ticket_count as number)         as tickets_day,       -- TICKET_COUNT → canónico
        cast(total_amount as float)          as gross_amount_day,  -- TOTAL_AMOUNT → canónico
        cast(avg_ticket_amount as float)     as avg_ticket,         -- AVG_TICKET_AMOUNT → canónico
        CONVERT_TIMEZONE('UTC', CAST(_FIVETRAN_SYNCED AS TIMESTAMP_TZ)) AS date_load_utc
    from source
)
select * from renamed
