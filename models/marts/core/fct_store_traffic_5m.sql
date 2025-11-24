{{ config(
    materialized='incremental',
    unique_key=['store_id', 'date_id', 'minute_5m_index'],
    on_schema_change='sync_all_columns'
) }}

with entries as (
  select
    e.store_id,
    cast(e.ts_5m as date)       as date_id,
    e.minute_5m_index,
    sum(case when d.direction_name = 'IN'  then e.people_count else 0 end) as people_in_count,
    sum(case when d.direction_name = 'OUT' then e.people_count else 0 end) as people_out_count
  from {{ ref('stg_franchise_script__entries_5m') }} e
  join {{ ref('stg_franchise_script__directions') }} d
    on e.direction_id = d.direction_id
  {% if is_incremental() %}
    where cast(e.ts_5m as date) > (
      select coalesce(max(date_id), cast('1900-01-01' as date))
      from {{ this }}
    )
  {% endif %}
  group by e.store_id, cast(e.ts_5m as date), e.minute_5m_index
),

queues as (
  select
    store_id,
    cast(ts_5m as date)   as date_id,
    minute_5m_index,
    cast(avg(queue_avg)  as number(12,2)) as queue_avg,
    cast(avg(wait_avg_s) as number(12,2)) as wait_avg_s
  from {{ ref('stg_franchise_script__queues_5m') }}
  {% if is_incremental() %}
    where cast(ts_5m as date) > (
      select coalesce(max(date_id), cast('1900-01-01' as date))
      from {{ this }}
    )
  {% endif %}
  group by store_id, cast(ts_5m as date), minute_5m_index
),

iot as (
  select
    store_id,
    cast(ts_5m as date)   as date_id,
    minute_5m_index,
    cast(avg(devices) as number(12,2)) as devices
  from {{ ref('stg_franchise_script__iot_presence_5m') }}
  {% if is_incremental() %}
    where cast(ts_5m as date) > (
      select coalesce(max(date_id), cast('1900-01-01' as date))
      from {{ this }}
    )
  {% endif %}
  group by store_id, cast(ts_5m as date), minute_5m_index
),

joined as (
  select
    e.store_id,
    e.date_id,
    e.minute_5m_index,
    e.people_in_count,
    e.people_out_count,
    q.queue_avg,
    q.wait_avg_s,
    i.devices
  from entries e
  left join queues q
    on  e.store_id        = q.store_id
    and e.date_id         = q.date_id
    and e.minute_5m_index = q.minute_5m_index
  left join iot i
    on  e.store_id        = i.store_id
    and e.date_id         = i.date_id
    and e.minute_5m_index = i.minute_5m_index
),

with_capacity as (
  select
    j.*,
    s.capacity_per_hour
  from joined j
  join {{ ref('dim_store') }} s
    on j.store_id = s.store_id
),

final_base as (
  select
    store_id,
    date_id,
    minute_5m_index,
    people_in_count,
    people_out_count,
    queue_avg,
    wait_avg_s,
    devices,
    capacity_per_hour,

    {{ arrival_rate_per_hour('people_in_count', 5) }}       as arrival_rate_ph,
    {{ utilization('arrival_rate_ph', 'capacity_per_hour') }} as utilization_5m
  from with_capacity
),

final as (
  select
    *,
    {{ pressure_index('utilization_5m', 'queue_avg') }} as pressure_index_5m
  from final_base
)

select * from final
