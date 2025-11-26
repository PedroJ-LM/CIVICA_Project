{{ config(
    materialized = 'view'
) }}

with base as (
    select
        store_id,
        day,
        direction_name,
        people_count,
        date_load_utc
    from {{ ref('stg_franchise_script__entries_5m') }}
),

agg as (
    select
        store_id,
        cast(day as date) as date_id,
        -- IN / OUT / TOTAL por día
        sum(case when direction_name = 'IN'  then people_count else 0 end) as people_in_day,
        sum(case when direction_name = 'OUT' then people_count else 0 end) as people_out_day,
        sum(people_count)                                                as people_total_day,
        max(date_load_utc)                                               as date_load_utc
    from base
    group by store_id, cast(day as date)
)

select *
from agg
