with src as (
  select *
  from {{ ref('franchise_script__zones_hierarchy') }}
),

typed as (
  select
    cast(upper(trim(zone_name_clean)) as string) as zone_name_clean,
    cast(upper(trim(region_name))     as string) as region_name,
    cast(upper(trim(country_name))    as string) as country_name
  from src
)

select *
from typed
