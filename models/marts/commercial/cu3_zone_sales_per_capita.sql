{{ config(materialized='table') }}

-- CU3: Ventas per cápita por zona
-- ¿Qué zonas venden más por habitante (ventas per cápita), agregando todas las tiendas de la zona?

with sales as (
    select
        fs.zone_id,
        dz.zone_name,
        dz.population,
        fs.region_id,
        dz.region_name,
        fs.country_id,
        dz.country_name,
        fs.date_id,
        fs.gross_amount_day
    from {{ ref('fct_store_sales_day') }} fs
    left join {{ ref('dim_zone') }} dz
      on fs.zone_id = dz.zone_id
),

agg as (
    select
        country_id,
        country_name,
        region_id,
        region_name,
        zone_id,
        zone_name,
        population,
        sum(gross_amount_day)                               as total_sales,
        sum(gross_amount_day) / nullif(population, 0)       as sales_per_capita
    from sales
    group by
        country_id,
        country_name,
        region_id,
        region_name,
        zone_id,
        zone_name,
        population
)

select *
from agg
