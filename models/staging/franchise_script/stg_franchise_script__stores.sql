-- Grain: (store_id)
-- Normaliza store_format → store_format_id y status → store_status_id.

with b as (
  select *
  from {{ ref('base_franchise_script__stores') }}
)

select
  store_id,
  store_name,
  zone_id,
  lat,
  lon,
  capacity_per_hour,
  iff(store_format is null or trim(store_format)='',
      null,
      to_varchar(md5(upper(trim(store_format))))
  ) as store_format_id,
  floor_area_m2,
  gate_count,
  opening_time,
  closing_time,
  iff(status is null or trim(status)='',
      null,
      to_varchar(md5(upper(trim(status))))
  ) as store_status_id,
  date_load_utc
from b

