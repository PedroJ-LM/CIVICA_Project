with b as (
  select store_format, date_load_utc, _fivetran_deleted
  from {{ ref('base_franchise_script__stores') }}
  where store_format is not null and trim(store_format) <> ''
)
select
  to_varchar(md5(upper(trim(store_format)))) as store_format_id,
  upper(trim(store_format))                  as store_format_name,
  max(date_load_utc)                         as date_load_utc,
  max(_fivetran_deleted)                     as _fivetran_deleted
from b
group by store_format