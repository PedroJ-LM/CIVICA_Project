{{ config(materialized='view') }}

with base as (
    select
        store_id,
        ts_5m,
        queue_avg,
        wait_avg_s
    from {{ ref('stg_franchise_script__queues_5m') }}
),

agg as (
    select
        store_id,
        cast(ts_5m as date) as date_id,
        avg(queue_avg)  as queue_avg,
        avg(wait_avg_s) as wait_avg_s
    from base
    group by store_id, cast(ts_5m as date)
)

select *
from agg
