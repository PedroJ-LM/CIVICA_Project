{{ config(materialized='view') }}

with base as (
    select
        e.store_id,
        cast(e.ts_5m as date) as date_id,
        d.direction_name,
        e.people_count
    from {{ ref('stg_franchise_script__entries_5m') }} e
    join {{ ref('stg_franchise_script__directions') }} d
      on e.direction_id = d.direction_id
),

agg as (
    select
        store_id,
        date_id,
        sum(case when direction_name = 'IN'  then people_count else 0 end) as people_in_count,
        sum(case when direction_name = 'OUT' then people_count else 0 end) as people_out_count
    from base
    group by store_id, date_id
)

select *
from agg
