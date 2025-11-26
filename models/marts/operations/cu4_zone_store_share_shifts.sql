-- CU4: Cambios de cuota de visitas entre tiendas dentro de cada zona de origen

-- Para cada zona de origen, ¿qué tiendas ganan o pierden cuota de flujo de visitas con el tiempo (share dentro de la zona)?

with base as (
    select
        m.origin_zone_id,
        m.dest_store_id,
        m.date_id,
        sum(m.arrivals) as arrivals
    from {{ ref('fct_mobility_od_day') }} m
    group by 1,2,3
),

zone_total as (
    select
        origin_zone_id,
        date_id,
        sum(arrivals) as total_arrivals_zone
    from base
    group by 1,2
),

with_share as (
    select
        b.origin_zone_id,
        b.dest_store_id,
        b.date_id,
        b.arrivals,
        z.total_arrivals_zone,
        b.arrivals::float / nullif(z.total_arrivals_zone, 0)::float as share_in_zone,
        lag(
          b.arrivals::float / nullif(z.total_arrivals_zone, 0)::float
        ) over (
          partition by b.origin_zone_id, b.dest_store_id
          order by b.date_id
        ) as prior_share_in_zone
    from base b
    join zone_total z
      on b.origin_zone_id = z.origin_zone_id
     and b.date_id        = z.date_id
),

deltas as (
    select
        ws.origin_zone_id,
        z.zone_name          as origin_zone_name,
        ws.dest_store_id,
        ds.store_name        as dest_store_name,
        ws.date_id,
        ws.share_in_zone,
        ws.prior_share_in_zone,
        ws.share_in_zone - ws.prior_share_in_zone as delta_share
    from with_share ws
    left join {{ ref('dim_store') }} ds
      on ws.dest_store_id = ds.store_id
    left join {{ ref('stg_franchise_script__zones') }} z
      on ws.origin_zone_id = z.zone_id
)

select *
from deltas
where prior_share_in_zone is not null
  and abs(delta_share) >= 0.05 -- umbral configurable
