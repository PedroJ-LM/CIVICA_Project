-- Grain: 1 row = 1 ticket
-- Staging: cast types, normalize enums, simple derivations (no joins/agg)

with source as (
    select * from {{ source('franchise_script', 'POS_TICKETS') }}
),

renamed as (
    select
        cast(ticket_id as string)           as ticket_id,
        cast(store_id as number)            as store_id,
        cast(opened_at as timestamp)        as opened_at,
        cast(closed_at as timestamp)        as closed_at,
        upper(cast(channel as string))      as channel,        -- IN_STORE / ONLINE ...
        cast(promo_code as string)          as promo_code,
        cast(total_amount as float)         as total_amount,
        -- derivadas sin cambiar el grano:
        cast(opened_at as date)             as day,
        datediff('second', opened_at, closed_at) as duration_sec,
        CONVERT_TIMEZONE('UTC', CAST(_FIVETRAN_SYNCED AS TIMESTAMP_TZ)) AS date_load_utc
    from source
)

select * from renamed

