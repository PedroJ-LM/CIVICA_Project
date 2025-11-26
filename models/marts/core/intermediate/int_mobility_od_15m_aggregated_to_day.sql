{{ config(
    materialized = 'view'
) }}

with base as (
    select
        origin_zone_id,
        dest_store_id,
        day,
        arrivals,
        date_load_utc
    from {{ ref('stg_franchise_script__mobility_od_15m') }}
),

agg as (
    select
        origin_zone_id,
        dest_store_id,
        cast(day as date) as date_id,
        sum(arrivals)     as arrivals_day,
        max(date_load_utc) as date_load_utc
    from base
    group by origin_zone_id, dest_store_id, cast(day as date)
)

select *
from agg
