{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_station', 'saison', 'grandeur_hydro_elab'], 'unique': True},
      {'columns': ['classification_tendance']},
      {'columns': ['saison']},
      {'columns': ['code_departement']}
    ],
    post_hook=[
      "{{ add_primary_key(['code_station', 'saison', 'grandeur_hydro_elab']) }}",
      "{{ add_foreign_key(['code_station'], 'stg_hydrometry_stations', ['code_station']) }}"
    ]
  )
}}

-- Table analytique dédiée aux tendances hydrométriques (par saison)
-- Source: fct_monthly_hydro

WITH monthly AS (
    SELECT * FROM {{ ref('fct_monthly_hydro') }}
),

-- Calcul des saisons
saisons AS (
    SELECT
        code_station,
        grandeur_hydro_elab,
        code_departement,
        nom_departement,
        mois,
        resultat_moyen,
        CASE 
            WHEN EXTRACT(MONTH FROM mois) IN (12, 1, 2) THEN 'HIVER'
            WHEN EXTRACT(MONTH FROM mois) IN (3, 4, 5) THEN 'PRINTEMPS'
            WHEN EXTRACT(MONTH FROM mois) IN (6, 7, 8) THEN 'ETE'
            WHEN EXTRACT(MONTH FROM mois) IN (9, 10, 11) THEN 'AUTOMNE'
        END AS saison
    FROM monthly
    WHERE mois >= CURRENT_DATE - INTERVAL '10 years'
),

tendances_saisonnieres AS (
    SELECT
        code_station,
        grandeur_hydro_elab,
        saison,
        MIN(code_departement) AS code_departement,
        MIN(nom_departement) AS nom_departement,
        COUNT(*) AS nb_points,
        REGR_SLOPE(resultat_moyen, EXTRACT(EPOCH FROM mois)) * 31536000 AS slope_par_an,
        REGR_R2(resultat_moyen, EXTRACT(EPOCH FROM mois)) AS r2
    FROM saisons
    GROUP BY code_station, grandeur_hydro_elab, saison
    HAVING COUNT(*) >= 5
)

SELECT
    ts.code_station,
    ts.grandeur_hydro_elab,
    ts.saison,
    ts.code_departement,
    ts.nom_departement,
    
    -- Métriques de tendance
    ts.slope_par_an AS variation_annuelle,
    ts.r2 AS fiabilite_tendance,
    ts.nb_points,
    
    -- Classification
    CASE 
        WHEN ts.slope_par_an > 0.05 THEN 'HAUSSE_FORTE'
        WHEN ts.slope_par_an > 0.01 THEN 'HAUSSE_LEGERE'
        WHEN ts.slope_par_an < -0.05 THEN 'BAISSE_FORTE'
        WHEN ts.slope_par_an < -0.01 THEN 'BAISSE_LEGERE'
        ELSE 'STABLE'
    END AS classification_tendance,
    
    -- Projection à 5 ans (simple extrapolation linéaire)
    ts.slope_par_an * 5 AS projection_variation_5ans
    
FROM tendances_saisonnieres ts
