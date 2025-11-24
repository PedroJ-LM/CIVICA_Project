{{ config(materialized = 'table') }}

with pos as (
  select
    store_id,
    day,
    tickets_day,
    gross_amount_day,
    avg_ticket,
    date_load_utc
  from {{ ref('stg_franchise_script__pos_store_days') }}
),

stores as (
  select
    store_id,
    zone_id
  from {{ ref('stg_franchise_script__stores') }}
),

weather as (
  select
    zone_id,
    day,
    t_avg_c,
    rain_mm,
    wind_kmh,
    weather_label_id,
    date_load_utc
  from {{ ref('stg_franchise_script__zone_weather_days') }}
),

joined as (
  select
    p.store_id,
    cast(p.day as date) as date_id,
    s.zone_id,
    p.tickets_day,
    p.gross_amount_day,
    p.avg_ticket,
    w.t_avg_c,
    w.rain_mm,
    w.wind_kmh,
    w.weather_label_id,
    greatest(p.date_load_utc, w.date_load_utc) as date_load_utc
  from pos p
  join stores s
    on p.store_id = s.store_id
  left join weather w
    on s.zone_id = w.zone_id
   and p.day     = w.day
)

select *
from joined
