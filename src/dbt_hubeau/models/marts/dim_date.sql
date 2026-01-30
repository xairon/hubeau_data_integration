{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['date'], 'unique': True},
      {'columns': ['year']},
      {'columns': ['month']},
      {'columns': ['week']}
    ],
    post_hook=[
      "{{ add_primary_key(['date']) }}"
    ]
  )
}}

-- Dimension temps (date) construite à partir des faits piézo et hydro

WITH date_bounds AS (
    SELECT
        MIN(min_date) AS min_date,
        MAX(max_date) AS max_date
    FROM (
        SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM {{ ref('hubeau_daily_chroniques') }}
        UNION ALL
        SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM {{ ref('hydro_daily_chroniques') }}
    ) bounds
),

dates AS (
    SELECT
        generate_series(min_date, max_date, interval '1 day')::date AS date
    FROM date_bounds
    WHERE min_date IS NOT NULL AND max_date IS NOT NULL
)

SELECT
    date,
    EXTRACT(YEAR FROM date)::integer AS year,
    EXTRACT(QUARTER FROM date)::integer AS quarter,
    EXTRACT(MONTH FROM date)::integer AS month,
    EXTRACT(WEEK FROM date)::integer AS week,
    EXTRACT(DOY FROM date)::integer AS day_of_year,
    EXTRACT(ISODOW FROM date)::integer AS iso_day_of_week,
    CASE WHEN EXTRACT(ISODOW FROM date) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM dates
