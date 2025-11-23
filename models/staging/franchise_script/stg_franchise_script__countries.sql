with base as (
  select distinct country_name
  from {{ ref('base_franchise_script__zones_hierarchy') }}
),

final as (
  select
    md5(country_name) as country_id,
    country_name
  from base
)

select *
from final
