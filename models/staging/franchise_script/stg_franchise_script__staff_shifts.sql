-- Grain: (staff_id, shift_start)
{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'merge',
    unique_key           = ['staff_id','shift_start'],
    on_schema_change     = 'sync_all_columns'
) }}

with b as (
  select *
  from {{ ref('base_franchise_script__staff_shifts') }}
  {% if is_incremental() %}
    where date_load_utc > (select max(date_load_utc) from {{ this }})
  {% endif %}
)

select
  staff_id,
  store_id,
  iff(role_name is null or trim(role_name) = '',
      null,
      to_varchar(md5(upper(trim(role_name))))
  ) as role_id,
  shift_start,
  shift_end,
  register_id,
  date_load_utc,
  _fivetran_deleted
from b

