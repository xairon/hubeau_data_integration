{{
  config(
    materialized = 'incremental',
    unique_key = ['latitude', 'longitude', 'era5_date'],
    incremental_strategy = 'delete+insert',
    incremental_predicates = [
      time_range_delete_predicate('era5_date', '30 days')
    ],
    indexes = [
      {'columns': ['era5_date'], 'type': 'brin'}
    ],
    post_hook = [
      "{{ add_primary_key(['latitude', 'longitude', 'era5_date']) }}",
      "{{ add_foreign_key(['latitude', 'longitude'], 'int_era5_grid_points', ['era5_latitude', 'era5_longitude']) }}"
    ]
  )
}}

-- ERA5 filtré sur les points de grille utilisés par TOUTES les stations (piézo + hydro).
-- Union des grid points des deux mappings pour éviter la duplication de données ERA5.
-- Incremental: ne traite que les nouvelles dates ERA5.

WITH all_station_grid_points AS (
    SELECT DISTINCT
        era5_latitude AS latitude,
        era5_longitude AS longitude
    FROM {{ ref('int_station_era5_mapping') }}

    UNION

    SELECT DISTINCT
        era5_latitude AS latitude,
        era5_longitude AS longitude
    FROM {{ ref('int_hydro_station_era5_mapping') }}
),

filtered_era5 AS (
    -- Précipitation + ETP depuis le snapshot horaire (flux d'accumulation : la valeur stockée
    -- est déjà la quantité journalière, pas de produit journalier dédié). Ce CTE PILOTE les
    -- lignes/dates ; la température est jointe séparément (voir daily_temp).
    -- Arrondi 0.1° à la lecture de silver : agrège les variantes flottantes d'une même
    -- cellule (48.1 historique + 48.09999999999995 incrémental) sous une seule coord,
    -- de sorte que les stations récupèrent tout l'historique. DISTINCT ON protège la PK
    -- au cas où deux variantes partageraient une date. cf. mémoire era5-coordinate-precision-bug.
    SELECT DISTINCT ON (ROUND(e.latitude::numeric, 1), ROUND(e.longitude::numeric, 1), e.time::date)
        ROUND(e.latitude::numeric, 1) AS latitude,
        ROUND(e.longitude::numeric, 1) AS longitude,
        e.time::date AS era5_date,
        e.total_precipitation::numeric AS total_precipitation,
        e.potential_evaporation::numeric AS potential_evaporation
    FROM {{ ref('stg_era5_timeseries') }} e
    INNER JOIN all_station_grid_points g
        ON ROUND(e.latitude::numeric, 1) = g.latitude
        AND ROUND(e.longitude::numeric, 1) = g.longitude
    WHERE e.total_precipitation IS NOT NULL
      AND e.potential_evaporation IS NOT NULL
    {% if is_incremental() %}
      AND e.time > (SELECT COALESCE(MAX(era5_date), '1900-01-01'::date) FROM {{ this }})
    {% endif %}
    ORDER BY ROUND(e.latitude::numeric, 1), ROUND(e.longitude::numeric, 1), e.time::date
),

daily_temp AS (
    -- Température = VRAIE moyenne journalière (t2m_mean), agrégée localement depuis l'archive
    -- horaire ERA5-Land. Remplace le snapshot 00:00 UTC biaisé (~2-4°C de biais froid l'été)
    -- que portait auparavant stg_era5_timeseries.temperature_2m. Même grain cellule 0.1° × jour
    -- → LEFT JOIN 1:1 en aval. Colonne exposée sous le nom temperature_2m : aucun changement
    -- de schéma pour les marts station consommateurs.
    SELECT DISTINCT ON (ROUND(d.latitude::numeric, 1), ROUND(d.longitude::numeric, 1), d.time::date)
        ROUND(d.latitude::numeric, 1) AS latitude,
        ROUND(d.longitude::numeric, 1) AS longitude,
        d.time::date AS era5_date,
        d.t2m_mean::numeric AS temperature_2m
    FROM {{ ref('stg_era5_daily_temp_stats') }} d
    INNER JOIN all_station_grid_points g
        ON ROUND(d.latitude::numeric, 1) = g.latitude
        AND ROUND(d.longitude::numeric, 1) = g.longitude
    {% if is_incremental() %}
    WHERE d.time > (SELECT COALESCE(MAX(era5_date), '1900-01-01'::date) FROM {{ this }})
    {% endif %}
    ORDER BY ROUND(d.latitude::numeric, 1), ROUND(d.longitude::numeric, 1), d.time::date
)

SELECT
    f.latitude,
    f.longitude,
    f.era5_date,
    dt.temperature_2m,
    f.total_precipitation,
    f.potential_evaporation
FROM filtered_era5 f
LEFT JOIN daily_temp dt
    ON dt.latitude = f.latitude
    AND dt.longitude = f.longitude
    AND dt.era5_date = f.era5_date
