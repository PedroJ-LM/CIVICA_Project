-- Base 1:1 con el source; tipado y estandarización de nombres.
with src as (
    select * from {{ source('franchise_script','STAFF_SHIFTS') }}
),
typed as (
    select
        cast(staff_id    as number)         as staff_id,
        cast(store_id    as number)         as store_id,
        upper(cast(role  as string))        as role_name,       
        cast(shift_start as timestamp_ntz)  as shift_start,
        cast(shift_end   as timestamp_ntz)  as shift_end,
        cast(register_id as number)         as register_id,
        --cast(date_trunc('day', shift_start) as date) as day,
        convert_timezone('UTC', _fivetran_synced) as date_load_utc,
        cast(_fivetran_deleted as string)         as _fivetran_deleted
    from src
)
select * from typed

