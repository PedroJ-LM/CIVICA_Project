with b as (
  select status, date_load_utc, _fivetran_deleted
  from {{ ref('base_franchise_script__stores') }}
  where status is not null and trim(status) <> ''
)
select
  to_varchar(md5(upper(trim(status)))) as store_status_id,
  upper(trim(status))                  as store_status_name,
  max(date_load_utc)                   as date_load_utc,
  max(_fivetran_deleted)               as _fivetran_deleted
from b
group by status
