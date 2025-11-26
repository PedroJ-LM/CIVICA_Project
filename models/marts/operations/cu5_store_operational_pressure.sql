-- CU5: Hotspots de presión operativa por tienda (media histórica)

-- ¿Qué tiendas están estructuralmente más “estresadas” (alta presión, alta utilización, colas largas) a lo largo del tiempo?

with base as (
    select
        t.store_id,
        ds.store_name,
        ds.zone_id,
        ds.region_id,
        ds.country_id,
        t.date_id,
        t.people_in_count,
        t.queue_avg,
        t.wait_avg_s,
        t.devices,
        t.arrival_rate_ph,
        t.utilization_5m    as utilization,
        t.pressure_index_5m as pressure_index
    from {{ ref('fct_store_traffic_day') }} t
    join {{ ref('dim_store') }} ds
      on t.store_id = ds.store_id
),

agg as (
    select
        store_id,
        store_name,
        zone_id,
        region_id,
        country_id,
        avg(pressure_index)  as avg_pressure_index,
        avg(utilization)     as avg_utilization,
        avg(queue_avg)       as avg_queue,
        avg(wait_avg_s)      as avg_wait_seconds,
        avg(people_in_count) as avg_daily_entries
    from base
    group by 1,2,3,4,5
)

select *
from agg
order by avg_pressure_index desc
