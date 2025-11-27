{% macro utilization(arrival_rate_expr, capacity_expr) %}
  (
    {{ arrival_rate_expr }}::float
    / nullif({{ capacity_expr }}::float, 0.0)
  )
{% endmacro %}

{% macro pressure_index(utilization_expr, queue_avg_expr) %}
  (
    {{ utilization_expr }}::float
    * (1.0 + {{ queue_avg_expr }}::float / ({{ queue_avg_expr }}::float + 1.0))
  )
{% endmacro %}

{% macro gravity_score(flow_expr, distance_km_expr, alpha=1.0, beta=2.0) %}
  (
    power({{ flow_expr }}::float, {{ alpha }})
    / nullif(power({{ distance_km_expr }}::float, {{ beta }}), 0.0)
  )
{% endmacro %}

{# ----------------- GEO / DISTANCIAS ----------------- #}

{% macro distance_haversine_km(lat1_expr, lon1_expr, lat2_expr, lon2_expr) %}
  (
    2 * 6371
    * asin(
        sqrt(
          pow(
            sin(
              (radians({{ lat2_expr }}) - radians({{ lat1_expr }})) / 2
            ),
            2
          )
          + cos(radians({{ lat1_expr }}))
          * cos(radians({{ lat2_expr }}))
          * pow(
            sin(
              (radians({{ lon2_expr }}) - radians({{ lon1_expr }})) / 2
            ),
            2
          )
        )
      )
  )
{% endmacro %}
