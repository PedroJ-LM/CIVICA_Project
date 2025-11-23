-- stg_franchise_script__pos_tickets.sql

with base as (
  select * from {{ ref('base_franchise_script__pos_tickets') }}
),
final as (
  select
    ticket_id,
    store_id,
    opened_at,
    closed_at,
    upper(trim(channel))                     as channel_name,
    md5(upper(trim(channel)))                as channel_id,

    case when promo_code is null or trim(promo_code) = '' then null
         else upper(promo_code)
    end                                      as promo_code,

    case when promo_code is null or trim(promo_code) = '' then null
         else md5(upper(trim(promo_code)))  
    end                                      as promo_id,

    total_amount,
    cast(date_trunc('day', opened_at) as date) as day,
    datediff('second', opened_at, closed_at)   as duration_sec,

    date_load_utc
  from base
)
select * from final






