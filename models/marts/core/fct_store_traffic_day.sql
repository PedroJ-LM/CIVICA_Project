{{ config(
    materialized='incremental',
    unique_key=['store_id', 'date_id'],
    on_schema_change='sync_all_columns'
) }}

with entries as (
    select
        store_id,
        date_id,
        people_in_count,
        people_out_count
    from {{ ref('int_entries_5m_aggregated_to_day') }}
    {% if is_incremental() %}
      where date_id > (
        select coalesce(max(date_id), cast('1900-01-01' as date))
        from {{ this }}
      )
    {% endif %}
),

queues as (
    select
        store_id,
        date_id,
        queue_avg,
        wait_avg_s
    from {{ ref('int_queues_5m_aggregated_to_day') }}
    {% if is_incremental() %}
      where date_id > (
        select coalesce(max(date_id), cast('1900-01-01' as date))
        from {{ this }}
      )
    {% endif %}
),

iot as (
    select
        store_id,
        date_id,
        devices
    from {{ ref('int_iot_presence_5m_aggregated_to_day') }}
    {% if is_incremental() %}
      where date_id > (
        select coalesce(max(date_id), cast('1900-01-01' as date))
        from {{ this }}
      )
    {% endif %}
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
        s.capacity_per_hour
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
        {{ arrival_rate_per_hour('people_in_count', 5) }}         as arrival_rate_ph,
        {{ utilization('arrival_rate_ph', 'capacity_per_hour') }} as utilization_5m
    from with_capacity
),

final as (
    select
        *,
        {{ pressure_index('utilization_5m', 'queue_avg') }} as pressure_index_5m
    from final_base
)

select *
from final
