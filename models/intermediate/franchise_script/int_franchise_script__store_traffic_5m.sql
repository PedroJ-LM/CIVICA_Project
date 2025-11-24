{{ config(materialized = 'view') }}

with entries as (
  select
    store_id,
    ts_5m,
    sum(case when direction_id = {{ dbt_utils.generate_surrogate_key(["'IN'"]) }} then people_count else 0 end)  as people_in_count,
    sum(case when direction_id = {{ dbt_utils.generate_surrogate_key(["'OUT'"]) }} then people_count else 0 end) as people_out_count
  from {{ ref('stg_franchise_script__entries_5m') }}
  group by store_id, ts_5m
),

queues as (
  select
    store_id,
    ts_5m,
    queue_avg,
    wait_avg_s
  from {{ ref('stg_franchise_script__queues_5m') }}
),

iot as (
  select
    store_id,
    ts_5m,
    devices
  from {{ ref('stg_franchise_script__iot_presence_5m') }}
),

final as (
  select
    coalesce(e.store_id, q.store_id, i.store_id) as store_id,
    coalesce(e.ts_5m,    q.ts_5m,    i.ts_5m)    as ts_5m,

    e.people_in_count,
    e.people_out_count,
    q.queue_avg,
    q.wait_avg_s,
    i.devices
  from entries e
  full outer join queues q
    on e.store_id = q.store_id
   and e.ts_5m    = q.ts_5m
  full outer join iot i
    on coalesce(e.store_id, q.store_id) = i.store_id
   and coalesce(e.ts_5m,    q.ts_5m)    = i.ts_5m
)

select * from final
