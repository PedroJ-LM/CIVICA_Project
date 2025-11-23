with b as (
  select role_name, date_load_utc
  from {{ ref('base_franchise_script__staff_shifts') }}
  where role_name is not null and trim(role_name) <> ''
)
select
  to_varchar(md5(upper(trim(role_name)))) as role_id,
  upper(trim(role_name))                  as role_name,
  max(date_load_utc)                      as date_load_utc
from b
group by role_name
