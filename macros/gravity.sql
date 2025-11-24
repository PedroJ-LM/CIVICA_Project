{% macro haversine_km(lat1, lon1, lat2, lon2) -%}
  6371 * 2 * asin(
    sqrt(
      power(
        sin((radians({{ lat2 }}) - radians({{ lat1 }})) / 2),
        2
      )
      + cos(radians({{ lat1 }}))
        * cos(radians({{ lat2 }}))
        * power(
            sin((radians({{ lon2 }}) - radians({{ lon1 }})) / 2),
            2
          )
    )
  )
{%- endmacro %}

{% macro gravity_potential(population, income_index, floor_area_m2, distance_km, alpha=1.0, beta=1.0, gamma=2.0) -%}
  (
    power({{ population }} * {{ income_index }}, {{ alpha }})
    * power({{ floor_area_m2 }}, {{ beta }})
    / nullif(power({{ distance_km }}, {{ gamma }}), 0)
  )
{%- endmacro %}
