{{
  config(
    materialized = 'incremental',
    unique_key = ['code_bss', 'annee'],
    incremental_strategy = 'delete+insert',
    indexes = [
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
        
        -- Bilan hydrique simplifié (P - ETP), pondéré par le nombre réel de jours mesurés par mois
        SUM(precipitation_totale) - SUM(evaporation_moyenne * nb_jours_mesures) AS bilan_hydrique_annuel,
        
        -- Comptage
        SUM(nb_jours_mesures) AS nb_jours_mesures_annuel,
        COUNT(*) AS nb_mois_mesures,
        
        -- Coordonnées
        MIN(era5_latitude) AS era5_latitude,
        MIN(era5_longitude) AS era5_longitude
        
    FROM monthly
    GROUP BY code_bss, EXTRACT(YEAR FROM mois)
),

-- Étape 1: Calcul des percentiles (une seule fois)
yearly_with_percentiles AS (
    SELECT
        *,
        PERCENT_RANK() OVER (
            PARTITION BY code_bss ORDER BY niveau_moyen_annuel
        ) AS percentile_niveau_historique,
        PERCENT_RANK() OVER (
            PARTITION BY code_bss ORDER BY precipitation_totale_annuelle
        ) AS percentile_precipitation_historique,
        LAG(niveau_moyen_annuel) OVER (
            PARTITION BY code_bss ORDER BY annee
        ) AS niveau_annee_prec,
        LAG(precipitation_totale_annuelle) OVER (
            PARTITION BY code_bss ORDER BY annee
        ) AS precipitation_annee_prec
    FROM yearly_agg
)

SELECT
    code_bss,
    annee,
    code_departement,
    nom_departement,
    code_eh,
    libelle_eh,
    niveau_moyen_annuel,
    niveau_min_annuel,
    niveau_max_annuel,
    amplitude_annuelle,
    niveau_stddev_annuel,
    profondeur_moyenne_annuelle,
    temperature_moyenne_annuelle,
    temperature_min_annuelle,
    temperature_max_annuelle,
    precipitation_totale_annuelle,
    evaporation_moyenne_annuelle,
    bilan_hydrique_annuel,
    nb_jours_mesures_annuel,
    nb_mois_mesures,
    era5_latitude,
    era5_longitude,

    -- Variation vs année précédente
    niveau_moyen_annuel - niveau_annee_prec AS variation_niveau_vs_annee_prec,
    precipitation_totale_annuelle - precipitation_annee_prec AS variation_precipitation_vs_annee_prec,

    -- Pourcentage de variation
    CASE
        WHEN niveau_annee_prec IS NOT NULL AND niveau_annee_prec != 0
        THEN (niveau_moyen_annuel - niveau_annee_prec) / ABS(niveau_annee_prec) * 100
        ELSE NULL
    END AS variation_niveau_pct,

    -- Percentiles (calculés une seule fois dans la CTE)
    percentile_niveau_historique,
    percentile_precipitation_historique,

    -- Classification de l'année (sécheresse/normal/humide)
    CASE
        WHEN percentile_niveau_historique < 0.2 THEN 'TRES_BAS'
        WHEN percentile_niveau_historique < 0.4 THEN 'BAS'
        WHEN percentile_niveau_historique < 0.6 THEN 'NORMAL'
        WHEN percentile_niveau_historique < 0.8 THEN 'HAUT'
        ELSE 'TRES_HAUT'
    END AS classification_niveau_annuel,

    -- Moyenne mobile 5 ans
    AVG(niveau_moyen_annuel) OVER (
        PARTITION BY code_bss
        ORDER BY annee
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS niveau_moy_mobile_5ans

FROM yearly_with_percentiles
