{{ config(
    materialized='table'
) }}

with entries as (
    select
        store_id,
        date_id,
        people_in_count,
        people_out_count
    from {{ ref('int_entries_5m_aggregated_to_day') }}
),

queues as (
    select
        store_id,
        date_id,
        queue_avg,
        wait_avg_s
    from {{ ref('int_queues_5m_aggregated_to_day') }}
),

iot as (
    select
        store_id,
        date_id,
        devices
    from {{ ref('int_iot_presence_5m_aggregated_to_day') }}
),

joined as (
    select
        e.store_id,
        e.date_id,
        e.people_in_count,
        e.people_out_count,
        q.queue_avg,
        q.wait_avg_s,
        i.devices
    from entries e
    left join queues q
      on  e.store_id = q.store_id
      and e.date_id  = q.date_id
    left join iot i
      on  e.store_id = i.store_id
      and e.date_id  = i.date_id
),

with_capacity as (
    select
        j.store_id,
        j.date_id,
        j.people_in_count,
        j.people_out_count,
        j.queue_avg,
        j.wait_avg_s,
        j.devices,
        s.zone_id,
        z.region_id,
        z.country_id,
        s.capacity_per_hour,
        s.opening_time,
        s.closing_time
    from joined j
    join {{ ref('dim_store') }} s
      on j.store_id = s.store_id
    left join {{ ref('dim_zone') }} z
      on s.zone_id = z.zone_id
),

final_base as (
    select
        store_id,
        date_id,
        zone_id,
        region_id,
        country_id,
        people_in_count,
        people_out_count,
        queue_avg,
        wait_avg_s,
        devices,
        capacity_per_hour,

        -- entradas medias por hora del día:
        -- total de entradas / horas de apertura
        round(
          people_in_count::float
            / nullif(datediff('minute', opening_time, closing_time) / 60.0, 0.0),
          2
        ) as arrival_rate_ph,

        {{ utilization('arrival_rate_ph', 'capacity_per_hour') }} as utilization
    from with_capacity
),

final as (
    select
        store_id,
        date_id,
        zone_id,
        region_id,
        country_id,
        people_in_count,
        people_out_count,
        arrival_rate_ph,
        round(queue_avg,   2) as queue_avg,
        round(wait_avg_s,  2) as wait_avg_s,
        round(devices,     0) as devices,
        capacity_per_hour,
        round(utilization,  2) as utilization,
        round({{ pressure_index('utilization', 'queue_avg') }}, 2)
            as pressure_index
    from final_base
)

select *
from final
