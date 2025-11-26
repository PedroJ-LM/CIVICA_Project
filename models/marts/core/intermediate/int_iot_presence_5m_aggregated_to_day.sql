{{ config(
    materialized = 'view'
) }}

with base as (
    select
        store_id,
        day,
        devices,
        rssi_mean,
        date_load_utc
    from {{ ref('stg_franchise_script__iot_presence_5m') }}
),

agg as (
    select
        store_id,
        cast(day as date) as date_id,
        avg(devices)      as devices_day,
        avg(rssi_mean)    as rssi_mean_day,
        max(date_load_utc) as date_load_utc
    from base
    group by store_id, cast(day as date)
)

select *
from agg
