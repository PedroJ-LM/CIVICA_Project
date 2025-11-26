{{ config(materialized='view') }}

with base as (
    select
        store_id,
        ts_5m,
        devices
    from {{ ref('stg_franchise_script__iot_presence_5m') }}
),

agg as (
    select
        store_id,
        cast(ts_5m as date) as date_id,
        avg(devices) as devices
    from base
    group by store_id, cast(ts_5m as date)
)

select *
from agg
