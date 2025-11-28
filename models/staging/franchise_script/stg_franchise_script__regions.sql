with b as (
  select
    region_name,
    country_name,
    date_load_utc,
    _fivetran_deleted
  from {{ ref('base_franchise_script__zones') }}
  where region_name  is not null
    and trim(region_name)  <> ''
    and country_name is not null
    and trim(country_name) <> ''
),

final as (
  select
    -- IDs consistentes con stg_zones y stg_countries
    to_varchar(md5(region_name))   as region_id,
    region_name,

    to_varchar(md5(country_name))  as country_id,

    -- metadatos a nivel región-país
    max(date_load_utc)             as date_load_utc,
    max(_fivetran_deleted)         as _fivetran_deleted
  from b
  group by
    region_name,
    country_name
)

select *
from final
