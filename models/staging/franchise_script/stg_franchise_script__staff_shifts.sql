-- Grain: 1 row = (staff_id, shift_start)  (no agregamos; 1:1 con el source)
-- Staging: tipado + normalización mínima. Sin joins/agg.

with source as (
    select * from {{ source('franchise_script', 'STAFF_SHIFTS') }}
),
renamed as (
    select
        cast(staff_id as number)            as staff_id,
        cast(store_id as number)            as store_id,
        upper(cast(role as string))         as role,
        cast(shift_start as timestamp)      as shift_start,
        cast(shift_end as timestamp)        as shift_end,
        cast(register_id as number)         as register_id,   -- si tu Bronze usa otro nombre (p.ej. till_id), ajusta aquí
        cast(shift_start as date)           as day,
        CONVERT_TIMEZONE('UTC', CAST(_FIVETRAN_SYNCED AS TIMESTAMP_TZ)) AS date_load_utc
    from source
)
select * from renamed
