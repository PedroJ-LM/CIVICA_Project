with base as (
  select channel, date_load_utc, _fivetran_deleted
  from {{ ref('base_franchise_script__pos_tickets') }}
  where channel is not null
    and trim(channel) <> ''
),
agg as (
  select
    upper(trim(channel))           as channel_name,
    md5(upper(trim(channel)))      as channel_id,
    max(date_load_utc)             as date_load_utc,
    max(_fivetran_deleted)         as _fivetran_deleted
  from base
  group by upper(trim(channel))
)
select
  channel_id,
  channel_name,
  date_load_utc,
  _fivetran_deleted
from agg



