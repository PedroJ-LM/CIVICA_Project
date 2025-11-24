{{ config(materialized='table') }}

with base as (

  -- Rango amplio para reutilización
  {{ dbt_date.get_date_dimension('2010-01-01', '2040-12-31') }}

),

final as (

  select
    -- Clave técnica de la dimensión (DATE), coherente con tus facts
    cast(date_day as date) as date_id,

    -- Alias "de negocio" alineados con tu schema.yml
    year_number   as year,
    month_of_year as month,
    day_of_month,
    day_of_week,

    case
      when day_of_week in (6, 7) then true
      else false
    end as is_weekend,

    -- A partir de aquí, TODAS las columnas que ya genera dbt_date
    -- (excepto las que ya hemos usado arriba con otros nombres)

    date_day,
    prior_date_day,
    next_date_day,
    prior_year_date_day,
    prior_year_over_year_date_day,
    day_of_week_iso,
    day_of_week_name,
    day_of_week_name_short,
    day_of_year,
    week_start_date,
    week_end_date,
    prior_year_week_start_date,
    prior_year_week_end_date,
    week_of_year,
    iso_week_start_date,
    iso_week_end_date,
    prior_year_iso_week_start_date,
    prior_year_iso_week_end_date,
    iso_week_of_year,
    prior_year_week_of_year,
    prior_year_iso_week_of_year,
    month_name,
    month_name_short,
    month_start_date,
    month_end_date,
    prior_year_month_start_date,
    prior_year_month_end_date,
    quarter_of_year,
    quarter_start_date,
    quarter_end_date,
    year_number,
    year_start_date,
    year_end_date

  from base

)

select * from final
