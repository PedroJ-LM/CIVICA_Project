-- Grano: 1 fila = 1 puerta (gate) de tienda en el inventario.
-- Objetivo: tipar y normalizar categorías (direction).

with source as (

    select * from {{ source('franchise_script', 'STORE_GATES') }}

),

renamed as (

    select
        cast(store_id as number)           as store_id,
        cast(gate_id as number)            as gate_id,
        gate_name                           as gate_name,
        upper(cast(direction as string))   as direction,   -- IN / OUT
        upper(cast(technology as string))  as technology,   -- dejamos sin "accepted_values" por ahora
        CONVERT_TIMEZONE('UTC', CAST(_FIVETRAN_SYNCED AS TIMESTAMP_TZ)) AS date_load_utc
    from source

)

select * from renamed
