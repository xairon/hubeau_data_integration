{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_bss', 'date'], 'unique': True},
      {'columns': ['dataset_split']}
    ],
    post_hook=[
      "{{ add_primary_key(['code_bss', 'date']) }}"
    ]
  )
}}

-- Dataset ML avec split train/validation/test (piézométrie)

WITH features AS (
    SELECT * FROM {{ ref('ml_features_piezo_daily') }}
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
