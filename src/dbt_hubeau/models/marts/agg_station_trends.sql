{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_bss'], 'unique': True},
      {'columns': ['classification_tendance']},
      {'columns': ['saison']},
      {'columns': ['code_departement']}
    ]
  )
}}

-- Table analytique dédiée aux tendances de long terme
-- Permet des analyses plus fines que dim_piezo_stations (par saison)
-- Source: fct_monthly_chroniques

WITH monthly AS (
    SELECT * FROM {{ ref('fct_monthly_chroniques') }}
),

-- Calcul des saisons
saisons AS (
    SELECT
        code_bss,
        mois,
        niveau_moyen,
        CASE 
            WHEN EXTRACT(MONTH FROM mois) IN (12, 1, 2) THEN 'HIVER'
            WHEN EXTRACT(MONTH FROM mois) IN (3, 4, 5) THEN 'PRINTEMPS'
            WHEN EXTRACT(MONTH FROM mois) IN (6, 7, 8) THEN 'ETE'
            WHEN EXTRACT(MONTH FROM mois) IN (9, 10, 11) THEN 'AUTOMNE'
        END AS saison
    FROM monthly
    WHERE mois >= CURRENT_DATE - INTERVAL '10 years'  -- Analyse sur 10 ans
),

tendances_saisonnieres AS (
    SELECT
        code_bss,
        saison,
        COUNT(*) AS nb_points,
        REGR_SLOPE(niveau_moyen, EXTRACT(EPOCH FROM mois)) * 31536000 AS slope_m_par_an, -- Conversion en mètres/an
        REGR_R2(niveau_moyen, EXTRACT(EPOCH FROM mois)) AS r2
    FROM saisons
    GROUP BY code_bss, saison
    HAVING COUNT(*) >= 5  -- Au moins 5 points saisonniers
),

metadata AS (
    SELECT code_bss, code_departement, nom_departement
    FROM {{ ref('stg_piezo_stations') }}
)

SELECT
    ts.code_bss,
    ts.saison,
    m.code_departement,
    m.nom_departement,
    
    -- Métriques de tendance
    ts.slope_m_par_an AS variation_annuelle_m,
    ts.r2 AS fiabilite_tendance,
    
    -- Classification
    CASE 
        WHEN ts.slope_m_par_an > 0.05 THEN 'HAUSSE_FORTE'
        WHEN ts.slope_m_par_an > 0.01 THEN 'HAUSSE_LEGERE'
        WHEN ts.slope_m_par_an < -0.05 THEN 'BAISSE_FORTE'
        WHEN ts.slope_m_par_an < -0.01 THEN 'BAISSE_LEGERE'
        ELSE 'STABLE'
    END AS classification_tendance,
    
    -- Projection à 5 ans (simple extrapolation linéaire)
    -- Attention: purement indicatif, ne prend pas en compte les modèles hydrogéologiques
    ts.slope_m_par_an * 5 AS projection_variation_5ans_m
    
FROM tendances_saisonnieres ts
LEFT JOIN metadata m ON ts.code_bss = m.code_bss
