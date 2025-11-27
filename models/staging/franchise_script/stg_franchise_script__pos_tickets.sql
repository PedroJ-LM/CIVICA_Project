{{ config(
    materialized        = 'incremental',
    incremental_strategy = 'merge',
    unique_key          = 'ticket_id',
    on_schema_change    = 'sync_all_columns'
) }}

with base as (
  select * from {{ ref('base_franchise_script__pos_tickets') }}
  {% if is_incremental() %}
    where date_load_utc > (select max(date_load_utc) from {{ this }})
  {% endif %}
),
final as (
  select
    ticket_id,
    store_id,
    opened_at,
    closed_at,
    --upper(trim(channel))                     as channel_name,
    md5(upper(trim(channel)))                as channel_id,
    -- metemos NO_PROMO cuando venga null/vacío
    --case 
    --  when promo_code is null or trim(promo_code) = '' then 'NO_PROMO'
    --  else upper(promo_code)
    --end                                      as promo_code,

    case 
      when promo_code is null or trim(promo_code) = '' then md5('NO_PROMO')
      else md5(upper(trim(promo_code)))
    end                                      as promo_id,

    total_amount,
    cast(date_trunc('day', opened_at) as date) as day,
    datediff('second', opened_at, closed_at)   as duration_sec,
    date_load_utc,
    _fivetran_deleted
  from base
)
select * from final







