{{ config(materialized='table') }}

-- CU1: Impacto del clima en las ventas diarias por tienda

-- ¿Cómo cambia la venta diaria media por tienda según el tipo de clima (bucket)
-- y si el día es lluvioso / muy caluroso / muy frío,
-- pudiendo filtrar también por país / región / zona?

with sales as (
    select
        fs.store_id,
        ds.store_name,

        -- geografía denormalizada desde la pareja dim_store + dim_zone
        fs.zone_id,
        dz.zone_name,
        dz.region_id,
        dz.region_name,
        dz.country_id,
        dz.country_name,

        fs.date_id,
        dd.year,
        dd.month,
        dd.day_of_week,

        fs.tickets_day,
        fs.gross_amount_day,
        fs.avg_ticket,

        fs.weather_bucket,
        fs.is_rainy,
        fs.is_hot,
        fs.is_cold
    from {{ ref('fct_store_sales_day') }} fs
    join {{ ref('dim_store') }} ds
      on fs.store_id = ds.store_id
    left join {{ ref('dim_zone') }} dz       -- nueva dimensión geográfica
      on fs.zone_id = dz.zone_id
    join {{ ref('dim_date') }} dd
      on fs.date_id = dd.date_id
),

agg as (
    select
        -- contexto geo completo para poder filtrar en la capa de BI
        country_id,
        country_name,
        region_id,
        region_name,
        zone_id,
        zone_name,
        store_id,
        store_name,

        weather_bucket,
        is_rainy,
        is_hot,
        is_cold,

        round( avg(gross_amount_day) , 2) as avg_daily_sales,
        round( avg(tickets_day) , 0)      as avg_daily_tickets,
        round( avg(avg_ticket) , 2)       as avg_ticket_amount
    from sales
    group by
        country_id,
        country_name,
        region_id,
        region_name,
        zone_id,
        zone_name,
        store_id,
        store_name,
        weather_bucket,
        is_rainy,
        is_hot,
        is_cold
)

select *
from agg
