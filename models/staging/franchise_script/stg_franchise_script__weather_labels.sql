-- Catálogo de etiquetas meteorológicas 

with base as (
  select weather_label, date_load_utc, _fivetran_deleted
  from {{ ref('base_franchise_script__zone_weather_days') }}
),
dedup as (
  select
    md5(upper(trim(weather_label))) as weather_label_id,
    weather_label,
    max(date_load_utc) as date_load_utc,
    max(_fivetran_deleted) as _fivetran_deleted
  from base
  group by 1,2
)
select * from dedup



