{% snapshot snap_franchise_script__zones %}

{{
  config(
    target_schema = 'SNAPSHOTS',
    unique_key    = 'zone_id',
    strategy      = 'timestamp',
    updated_at    = 'date_load_utc',
    hard_deletes  = 'invalidate'   
  )
}}

select *
from {{ ref('base_franchise_script__zones') }}

{% endsnapshot %}
