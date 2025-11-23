with base as (
  select * from {{ ref('base_franchise_script__store_gates') }}
),
final as (
  select
    store_id,
    gate_id,
    gate_name,
    direction,
    md5(upper(trim(direction))) as direction_id,
    technology,
    date_load_utc,
    _fivetran_deleted
  from base
)
select * from final
