with b as (
  select promo_code, date_load_utc, _fivetran_deleted
  from {{ ref('base_franchise_script__pos_tickets') }}
  where promo_code is not null and trim(promo_code) <> ''
)
select
  to_varchar(md5(upper(trim(promo_code)))) as promo_id,
  upper(trim(promo_code))                  as promo_code,
  max(date_load_utc)                       as date_load_utc,
  max(_fivetran_deleted)                   as _fivetran_deleted
from b
group by promo_code
