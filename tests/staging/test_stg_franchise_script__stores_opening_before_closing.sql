-- Falla si alguna tienda tiene opening_time >= closing_time
select *
from {{ ref('stg_franchise_script__stores') }}
where opening_time is not null
  and closing_time is not null
  and opening_time >= closing_time
