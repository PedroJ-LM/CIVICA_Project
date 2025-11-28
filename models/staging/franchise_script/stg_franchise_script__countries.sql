with b as (
  select
    country_name,
    date_load_utc,
    _fivetran_deleted
  from {{ ref('base_franchise_script__zones') }}
  where country_name is not null
    and trim(country_name) <> ''
),

final as (
  select
    to_varchar(md5(country_name)) as country_id,
    country_name,
    max(date_load_utc)            as date_load_utc,
    max(_fivetran_deleted)        as _fivetran_deleted
  from b
  group by country_name
)

select *
from final
