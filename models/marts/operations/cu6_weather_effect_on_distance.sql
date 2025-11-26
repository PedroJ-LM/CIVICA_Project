-- CU6: Efecto del clima en la distancia recorrida hasta la tienda

-- ¿En días de mal tiempo la gente tiende a ir a tiendas más cercanas, y en buen tiempo se desplaza más lejos?

-- Clasificamos flujos en NEAR / MID / FAR y miramos la distribución por weather_bucket.

with base as (
    select
        m.date_id,
        m.origin_zone_id,
        m.dest_store_id,
        m.arrivals,
        m.distance_km,
        m.weather_bucket,
        m.is_rainy,
        m.is_hot,
        m.is_cold
    from {{ ref('fct_mobility_od_day') }} m
),

bucketed as (
    select
        *,
        case
            when distance_km < 150  then 'NEAR'
            when distance_km < 300 then 'MID'
            else 'FAR'
        end as distance_bucket
    from base
),

agg as (
    select
        weather_bucket,
        distance_bucket,
        sum(arrivals) as arrivals
    from bucketed
    group by 1,2
),

total_weather as (
    select
        weather_bucket,
        sum(arrivals) as total_arrivals
    from agg
    group by 1
),

with_share as (
    select
        a.weather_bucket,
        a.distance_bucket,
        a.arrivals,
        t.total_arrivals,
        round( ( a.arrivals::float / nullif(t.total_arrivals, 0)::float ) * 100 , 2)  as pct_arrivals
    from agg a
    join total_weather t
      on a.weather_bucket = t.weather_bucket
)

select *
from with_share
order by weather_bucket, distance_bucket
