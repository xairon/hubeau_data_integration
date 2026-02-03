{{
  config(
    materialized = 'incremental',
    unique_key = ['code_bss', 'annee'],
    incremental_strategy = 'delete+insert',
    indexes = [
      {'columns': ['code_bss', 'annee'], 'unique': True},
      {'columns': ['code_bss']},
      {'columns': ['annee'], 'type': 'brin'},
      {'columns': ['code_departement']}
    ],
    post_hook = [
      "{{ add_primary_key(['code_bss', 'annee']) }}",
      "{{ convert_to_hypertable('annee', '10') }}",
      "{{ add_foreign_key(['code_bss'], 'stg_piezo_stations', ['code_bss']) }}"
    ]
  )
}}

-- Statistiques annuelles par station piézométrique
-- Source: fct_monthly_chroniques
-- Calcule synthèse annuelle + comparaisons inter-annuelles + percentiles historiques
-- INCREMENTAL: recalcul complet des stations récentes (toutes années).

{% set lookback_days = var('streaming_lookback_days', 7) %}

WITH recent_stations AS (
    SELECT DISTINCT code_bss
    FROM {{ ref('hubeau_daily_chroniques') }}
    WHERE date >= CURRENT_DATE - INTERVAL '{{ lookback_days }} days'
),
monthly AS (
    SELECT * FROM {{ ref('fct_monthly_chroniques') }}
    {% if is_incremental() %}
    WHERE code_bss IN (SELECT code_bss FROM recent_stations)
    {% endif %}
),

yearly_agg AS (
    SELECT
        code_bss,
        EXTRACT(YEAR FROM mois)::integer AS annee,
        
        -- Métadonnées station
        MIN(code_departement) AS code_departement,
        MIN(nom_departement) AS nom_departement,
        MIN(code_eh) AS code_eh,
        MIN(libelle_eh) AS libelle_eh,
        
        -- Piézométrie - Stats annuelles
        AVG(niveau_moyen) AS niveau_moyen_annuel,
        MIN(niveau_min) AS niveau_min_annuel,
        MAX(niveau_max) AS niveau_max_annuel,
        MAX(niveau_max) - MIN(niveau_min) AS amplitude_annuelle,
        STDDEV(niveau_moyen) AS niveau_stddev_annuel,
        
        -- Profondeur
        AVG(profondeur_moyenne) AS profondeur_moyenne_annuelle,
        
        -- Météo - Stats annuelles
        AVG(temperature_moyenne) AS temperature_moyenne_annuelle,
        MIN(temperature_min) AS temperature_min_annuelle,
        MAX(temperature_max) AS temperature_max_annuelle,
        SUM(precipitation_totale) AS precipitation_totale_annuelle,
        AVG(evaporation_moyenne) AS evaporation_moyenne_annuelle,
        
        -- Bilan hydrique simplifié (P - ETP)
        SUM(precipitation_totale) - SUM(evaporation_moyenne * 30) AS bilan_hydrique_annuel,
        
        -- Comptage
        SUM(nb_jours_mesures) AS nb_jours_mesures_annuel,
        COUNT(*) AS nb_mois_mesures,
        
        -- Coordonnées
        MIN(era5_latitude) AS era5_latitude,
        MIN(era5_longitude) AS era5_longitude
        
    FROM monthly
    GROUP BY code_bss, EXTRACT(YEAR FROM mois)
)

SELECT
    *,
    
    -- Variation vs année précédente
    niveau_moyen_annuel - LAG(niveau_moyen_annuel) OVER (
        PARTITION BY code_bss ORDER BY annee
    ) AS variation_niveau_vs_annee_prec,
    
    precipitation_totale_annuelle - LAG(precipitation_totale_annuelle) OVER (
        PARTITION BY code_bss ORDER BY annee
    ) AS variation_precipitation_vs_annee_prec,
    
    -- Pourcentage de variation
    CASE 
        WHEN LAG(niveau_moyen_annuel) OVER (PARTITION BY code_bss ORDER BY annee) IS NOT NULL
             AND LAG(niveau_moyen_annuel) OVER (PARTITION BY code_bss ORDER BY annee) != 0
        THEN (niveau_moyen_annuel - LAG(niveau_moyen_annuel) OVER (PARTITION BY code_bss ORDER BY annee)) 
             / ABS(LAG(niveau_moyen_annuel) OVER (PARTITION BY code_bss ORDER BY annee)) * 100
        ELSE NULL
    END AS variation_niveau_pct,
    
    -- Percentile historique (où se situe cette année par rapport à l'historique)
    PERCENT_RANK() OVER (
        PARTITION BY code_bss ORDER BY niveau_moyen_annuel
    ) AS percentile_niveau_historique,
    
    PERCENT_RANK() OVER (
        PARTITION BY code_bss ORDER BY precipitation_totale_annuelle
    ) AS percentile_precipitation_historique,
    
    -- Classification de l'année (sécheresse/normal/humide)
    CASE 
        WHEN PERCENT_RANK() OVER (PARTITION BY code_bss ORDER BY niveau_moyen_annuel) < 0.2 
        THEN 'TRES_BAS'
        WHEN PERCENT_RANK() OVER (PARTITION BY code_bss ORDER BY niveau_moyen_annuel) < 0.4 
        THEN 'BAS'
        WHEN PERCENT_RANK() OVER (PARTITION BY code_bss ORDER BY niveau_moyen_annuel) < 0.6 
        THEN 'NORMAL'
        WHEN PERCENT_RANK() OVER (PARTITION BY code_bss ORDER BY niveau_moyen_annuel) < 0.8 
        THEN 'HAUT'
        ELSE 'TRES_HAUT'
    END AS classification_niveau_annuel,
    
    -- Moyenne mobile 5 ans
    AVG(niveau_moyen_annuel) OVER (
        PARTITION BY code_bss 
        ORDER BY annee 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS niveau_moy_mobile_5ans
    
FROM yearly_agg
