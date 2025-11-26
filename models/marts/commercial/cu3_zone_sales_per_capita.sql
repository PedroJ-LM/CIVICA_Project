-- CU3: Ventas per cápita por zona

-- ¿Qué zonas venden más por habitante (ventas per cápita), agregando todas las tiendas de la zona?

with sales as (
    select
        ds.zone_id,
        ds.zone_name,
        ds.population,
        fs.date_id,
        fs.gross_amount_day
    from {{ ref('fct_store_sales_day') }} fs
    join {{ ref('dim_store') }} ds
      on fs.store_id = ds.store_id
),

agg as (
    select
        zone_id,
        zone_name,
        population,
        sum(gross_amount_day) as total_sales,
        sum(gross_amount_day) / nullif(population, 0) as sales_per_capita
    from sales
    group by 1,2,3
)

select *
from agg
