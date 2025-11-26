{{ config(materialized='view') }}

with base as (
    select
        origin_zone_id,
        dest_store_id,
        ts_15m,
        arrivals
    from {{ ref('stg_franchise_script__mobility_od_15m') }}
),

agg as (
    select
        origin_zone_id,
        dest_store_id,
        cast(ts_15m as date) as date_id,
        sum(arrivals) as arrivals
    from base
    group by origin_zone_id, dest_store_id, cast(ts_15m as date)
)

select *
from agg
