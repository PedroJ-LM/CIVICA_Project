-- Regla simple: capacidad/hora debería ser al menos 1 por puerta (umbral razonable)
-- Ajustable si tu dominio lo pide.
select *
from {{ ref('stg_franchise_script__stores') }}
where gate_count is not null
  and capacity_per_hour is not null
  and capacity_per_hour < 1 * greatest(gate_count, 1)