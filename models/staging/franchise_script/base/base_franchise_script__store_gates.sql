with src as (
  select * from {{ source('franchise_script', 'STORE_GATES') }}
),
typed as (
  select
    cast(store_id as number)    as store_id,
    cast(gate_id as number)     as gate_id,
    cast(gate_name as string)   as gate_name,
    cast(direction as string)   as direction,
    cast(technology as string)  as technology,
    CONVERT_TIMEZONE('UTC', _fivetran_synced) as date_load_utc,
    cast(_fivetran_deleted as string)         as _fivetran_deleted
  from src
)
select * from typed
