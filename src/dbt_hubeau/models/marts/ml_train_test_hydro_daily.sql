{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_station', 'date', 'grandeur_hydro_elab'], 'unique': True},
      {'columns': ['dataset_split']}
    ],
    post_hook=[
      "{{ add_primary_key(['code_station', 'date', 'grandeur_hydro_elab']) }}"
    ]
  )
}}

-- Dataset ML avec split train/validation/test (hydrométrie)

WITH features AS (
    SELECT * FROM {{ ref('ml_features_hydro_daily') }}
),

max_date AS (
    SELECT MAX(date) AS max_date FROM features
)

SELECT
    f.*,
    CASE
        WHEN f.date >= (SELECT max_date FROM max_date) - INTERVAL '365 days' THEN 'test'
        WHEN f.date >= (SELECT max_date FROM max_date) - INTERVAL '730 days' THEN 'validation'
        ELSE 'train'
    END AS dataset_split
FROM features f
