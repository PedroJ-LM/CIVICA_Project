-- CU1: Impacto del clima en las ventas diarias por tienda

-- ¿Cómo cambia la venta diaria media por tienda según el tipo de clima (bucket) y si el día es lluvioso / muy caluroso / muy frío?

with sales as (
    select
        fs.store_id,
        ds.store_name,
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
    join {{ ref('dim_date') }} dd
      on fs.date_id = dd.date_id
),

agg as (
    select
        store_id,
        store_name,
        weather_bucket,
        is_rainy,
        is_hot,
        is_cold,
        avg(gross_amount_day) as avg_daily_sales,
        avg(tickets_day)      as avg_daily_tickets,
        avg(avg_ticket)       as avg_ticket_amount
    from sales
    group by 1,2,3,4,5,6
)

select *
from agg
