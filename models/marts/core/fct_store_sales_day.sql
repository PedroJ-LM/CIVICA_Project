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

-- Ahora obtenemos la geografía combinando dim_store + dim_zone
stores as (
  select
    s.store_id,
    s.zone_id,
    z.region_id,
    z.country_id
  from {{ ref('dim_store') }} s
  left join {{ ref('dim_zone') }} z
    on s.zone_id = z.zone_id
),

-- Clima enriquecido por zona y día (intermediate de weather)
weather as (
  select
    zone_id,
    day,
    t_avg_c,
    rain_mm,
    wind_kmh,
    weather_label_id,
    weather_bucket,
    is_rainy,
    is_hot,
    is_cold,
    date_load_utc
  from {{ ref('int_zones_weather_days_enriched') }}
),

joined as (
  select
    p.store_id,
    cast(p.day as date)      as date_id,   -- FK a dim_date

    -- geografía de la tienda 
    s.zone_id,
    s.region_id,
    s.country_id,

    -- métricas de ventas
    p.tickets_day,
    p.gross_amount_day,
    p.avg_ticket,

    -- clima enriquecido
    w.t_avg_c,
    w.rain_mm,
    w.wind_kmh,
    w.weather_label_id,
    w.weather_bucket,
    w.is_rainy,
    w.is_hot,
    w.is_cold,

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
