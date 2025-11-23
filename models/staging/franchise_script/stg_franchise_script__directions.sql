with b as (
  select direction, date_load_utc, _fivetran_deleted
  from {{ ref('base_franchise_script__store_gates') }}
  where direction is not null
    and trim(direction) <> ''
),
agg as (
  select
    to_varchar(md5(upper(trim(direction)))) as direction_id,
    upper(trim(direction))                  as direction_name,
    max(date_load_utc)                      as date_load_utc,
    max(_fivetran_deleted)                  as _fivetran_deleted
  from b
  group by 1, 2          -- = direction_id, direction_name
)
select
  direction_id,
  direction_name,
  date_load_utc,
  _fivetran_deleted
from agg

