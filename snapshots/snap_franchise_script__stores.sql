{% snapshot snap_franchise_script__stores %}

{{
  config(
    target_schema = 'SNAPSHOTS',
    unique_key    = 'store_id',
    strategy      = 'timestamp',
    updated_at    = 'date_load_utc'
  )
}}

select *
from {{ ref('stg_franchise_script__stores') }}

{% endsnapshot %}
