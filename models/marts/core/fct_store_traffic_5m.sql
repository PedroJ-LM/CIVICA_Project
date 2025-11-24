{{ config(materialized = 'table') }}

with entries_raw as (
  select
    e.store_id,
    e.ts_5m,
    e.minute_5m_index,
    e.people_count,
    d.direction_name,
    e.date_load_utc
  from {{ ref('stg_franchise_script__entries_5m') }} e
  join {{ ref('stg_franchise_script__directions') }} d
    on e.direction_id = d.direction_id
),

entries_agg as (
  select
    store_id,
    ts_5m,
    minute_5m_index,
    sum(case when direction_name = 'IN'  then people_count else 0 end) as people_in_count,
    sum(case when direction_name = 'OUT' then people_count else 0 end) as people_out_count,
    max(date_load_utc) as date_load_utc
  from entries_raw
  group by store_id, ts_5m, minute_5m_index
),

queues as (
  select
    store_id,
    ts_5m,
    minute_5m_index,
    queue_avg,
    wait_avg_s,
    date_load_utc
  from {{ ref('stg_franchise_script__queues_5m') }}
),

iot as (
  select
    store_id,
    ts_5m,
    minute_5m_index,
    devices,
    date_load_utc
  from {{ ref('stg_franchise_script__iot_presence_5m') }}
),

-- Todas las combinaciones store + ts_5m que existan en cualquiera de las 3 series
slots as (
  select distinct store_id, ts_5m, minute_5m_index from entries_agg
  union
  select distinct store_id, ts_5m, minute_5m_index from queues
  union
  select distinct store_id, ts_5m, minute_5m_index from iot
),

final as (
  select
    s.store_id,
    cast(s.ts_5m as date) as date_id,
    s.ts_5m,
    s.minute_5m_index,

    coalesce(e.people_in_count,  0) as people_in_count,
    coalesce(e.people_out_count, 0) as people_out_count,

    q.queue_avg,
    q.wait_avg_s,
    i.devices,

    greatest(
      coalesce(e.date_load_utc, to_timestamp_ntz('1900-01-01')),
      coalesce(q.date_load_utc, to_timestamp_ntz('1900-01-01')),
      coalesce(i.date_load_utc, to_timestamp_ntz('1900-01-01'))
    ) as date_load_utc
  from slots s
  left join entries_agg e
    on s.store_id        = e.store_id
   and s.ts_5m           = e.ts_5m
   and s.minute_5m_index = e.minute_5m_index
  left join queues q
    on s.store_id        = q.store_id
   and s.ts_5m           = q.ts_5m
   and s.minute_5m_index = q.minute_5m_index
  left join iot i
    on s.store_id        = i.store_id
   and s.ts_5m           = i.ts_5m
   and s.minute_5m_index = i.minute_5m_index
)

select *
from final
