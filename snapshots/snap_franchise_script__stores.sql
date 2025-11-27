{% snapshot snap_franchise_script__stores %}

{{
  config(
    target_schema = 'SNAPSHOTS',
    unique_key    = 'store_id',
    strategy      = 'timestamp',
    updated_at    = 'date_load_utc',
    hard_deletes  = 'invalidate'
  )
}}

select *
from {{ ref('base_franchise_script__stores') }}

{% endsnapshot %}
