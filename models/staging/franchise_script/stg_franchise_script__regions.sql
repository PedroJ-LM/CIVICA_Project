with base as (
  select distinct
    region_name,
    country_name
  from {{ ref('base_franchise_script__zones_hierarchy') }}
),

final as (
  select
    md5(region_name)   as region_id,
    region_name,
    md5(country_name)  as country_id
  from base
)

select *
from final
