with src as (
  select * from {{ source('franchise_script', 'POS_TICKETS') }}
),
typed as (
  select
    cast(ticket_id as string)       as ticket_id,
    cast(store_id as number)        as store_id,
    cast(opened_at as timestamp)    as opened_at,
    cast(closed_at as timestamp)    as closed_at,
    upper(trim(cast(channel as string)))        as channel,
    cast(promo_code as string)      as promo_code,
    cast(total_amount as float)     as total_amount,
    CONVERT_TIMEZONE('UTC', _fivetran_synced) as date_load_utc,
    cast(_fivetran_deleted as string)         as _fivetran_deleted
  from src
)
select * from typed

