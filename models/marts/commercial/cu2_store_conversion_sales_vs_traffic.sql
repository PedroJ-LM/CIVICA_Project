-- CU2: Conversión de tráfico en tickets por tienda y día

-- Dado el tráfico de personas en tienda, ¿qué tasa de conversión a tickets tengo por día y tienda? ¿Cómo se relaciona con colas / espera?

with traffic as (
    select
        store_id,
        date_id,
        people_in_count,
        queue_avg,
        wait_avg_s,
        devices
    from {{ ref('fct_store_traffic_day') }}
),

sales as (
    select
        store_id,
        date_id,
        tickets_day,
        gross_amount_day,
        avg_ticket
    from {{ ref('fct_store_sales_day') }}
),

joined as (
    select
        s.store_id,
        ds.store_name,
        s.date_id,
        t.people_in_count,
        s.tickets_day,
        s.gross_amount_day,
        s.avg_ticket,
        case 
            when t.people_in_count > 0 
                then s.tickets_day::float / t.people_in_count::float
            else null
        end as conversion_rate,
        t.queue_avg,
        t.wait_avg_s,
        t.devices
    from sales s
    join traffic t
      on s.store_id = t.store_id
     and s.date_id  = t.date_id
    join {{ ref('dim_store') }} ds
      on s.store_id = ds.store_id
)

select *
from joined
