-- Grain: (staff_id, shift_start)
-- Normaliza role_name → role_id (hash). Sin joins.

with b as (
  select *
  from {{ ref('base_franchise_script__staff_shifts') }}
)

select
  staff_id,
  store_id,
  -- si hay texto -> id; si no hay -> null
  iff(role_name is null or trim(role_name) = '',
      null,
      to_varchar(md5(upper(trim(role_name))))
  ) as role_id,
  shift_start,
  shift_end,
  register_id,
  date_load_utc,
  _fivetran_deleted
from b

