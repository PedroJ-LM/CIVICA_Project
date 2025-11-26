{{ config(
    materialized = 'view'
) }}

with base as (
    select
        store_id,
        day,
        queue_avg,
        wait_avg_s,
        date_load_utc
    from {{ ref('stg_franchise_script__queues_5m') }}
),

agg as (
    select
        store_id,
        cast(day as date) as date_id,
        avg(queue_avg)   as queue_avg_day,
        avg(wait_avg_s)  as wait_avg_s_day,
        max(date_load_utc) as date_load_utc
    from base
    group by store_id, cast(day as date)
)

select *
from agg
