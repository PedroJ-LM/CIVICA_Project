{{ config(
    materialized     = 'incremental',
    unique_key       = ['store_id', 'date_id'],
    on_schema_change = 'sync_all_columns'
) }}

with entries as (
    select
        store_id,
        date_id,
        people_in_day,
        people_out_day,
        people_total_day,
        date_load_utc
    from {{ ref('int_entries_5m_aggregated_to_day') }}
),

queues as (
    select
        store_id,
        date_id,
        queue_avg_day,
        wait_avg_s_day,
        date_load_utc
    from {{ ref('int_queues_5m_aggregated_to_day') }}
),

iot as (
    select
        store_id,
        date_id,
        devices_day,
        rssi_mean_day,
        date_load_utc
    from {{ ref('int_iot_presence_5m_aggregated_to_day') }}
),

joined as (
    select
        e.store_id,
        e.date_id,

        e.people_in_day,
        e.people_out_day,
        e.people_total_day,

        q.queue_avg_day,
        q.wait_avg_s_day,

        i.devices_day,
        i.rssi_mean_day,

        greatest(e.date_load_utc,
                 q.date_load_utc,
                 i.date_load_utc) as date_load_utc
    from entries e
    left join queues q
      on e.store_id = q.store_id
     and e.date_id  = q.date_id
    left join iot i
      on e.store_id = i.store_id
     and e.date_id  = i.date_id
    {% if is_incremental() %}
    where e.date_id > (
        select coalesce(max(date_id), cast('1900-01-01' as date))
        from {{ this }}
    )
    {% endif %}
),

with_store as (
    select
        j.*,
        s.capacity_per_hour,

        -- atributos denormalizados de tienda (dim_store “engordada”)
        s.zone_id,
        s.zone_name,
        s.region_id,
        s.region_name,
        s.country_id,
        s.country_name,
        s.store_format_id,
        s.store_status_id
    from joined j
    join {{ ref('dim_store') }} s
      on j.store_id = s.store_id
),

final as (
    select
        store_id,
        date_id,

        -- dimensión tienda / zona / región / país
        zone_id,
        zone_name,
        region_id,
        region_name,
        country_id,
        country_name,
        store_format_id,
        store_status_id,

        -- métricas de tráfico diario
        people_in_day,
        people_out_day,
        people_total_day,
        queue_avg_day,
        wait_avg_s_day,
        devices_day,
        rssi_mean_day,

        capacity_per_hour,

        --  macros físicas
        {{ arrival_rate_per_hour('people_in_day', 1440) }}              as arrival_rate_ph_day,
        {{ utilization('arrival_rate_ph_day', 'capacity_per_hour') }}   as utilization_day,
        {{ pressure_index('utilization_day', 'queue_avg_day') }}        as pressure_index_day,

        date_load_utc
    from with_store
)

select *
from final
