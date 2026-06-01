{{
  config(
    materialized = 'incremental',
    unique_key = ['code_bss'],
    incremental_strategy = 'delete+insert',
    indexes = [
      {'columns': ['code_departement']},
      {'columns': ['tendance_classification']},
      {'columns': ['geom'], 'type': 'gist'}
    ],
    post_hook=[
      "{{ add_primary_key(['code_bss']) }}",
      "{{ add_foreign_key(['code_bss'], 'stg_piezo_stations', ['code_bss']) }}"
    ]
  )
}}

-- Dimension stations piézométriques enrichie
-- Combine métadonnées + statistiques calculées + tendances
-- Source: stg_piezo_stations + fct_yearly_stats + fct_monthly_chroniques
-- INCREMENTAL: recalcul des stations récentes uniquement.

{% set lookback_days = var('streaming_lookback_days', 7) %}

WITH recent_stations AS (
    SELECT DISTINCT code_bss
    FROM {{ ref('hubeau_daily_chroniques') }}
    WHERE date >= CURRENT_DATE - INTERVAL '{{ lookback_days }} days'
),
stations AS (
    SELECT * FROM {{ ref('stg_piezo_stations') }}
    {% if is_incremental() %}
    WHERE code_bss IN (SELECT code_bss FROM recent_stations)
    {% endif %}
),

-- Vraies dates de mesure (date quotidienne réelle, PAS le bucket mensuel).
-- derniere_mesure doit refléter le dernier relevé réel (ex. hier), pas MAX(mois).
real_dates AS (
    SELECT
        code_bss,
        MIN(date) AS premiere_mesure,
        MAX(date) AS derniere_mesure
    FROM {{ ref('hubeau_daily_chroniques') }}
    {% if is_incremental() %}
    WHERE code_bss IN (SELECT code_bss FROM recent_stations)
    {% endif %}
    GROUP BY code_bss
),

-- Stats globales par station (toute la période)
stats_globales AS (
    SELECT
        code_bss,
        SUM(nb_jours_mesures) AS nb_mesures_total,
        COUNT(*) AS nb_mois_total,
        
        -- Piézométrie - Stats globales
        AVG(niveau_moyen) AS niveau_moyen_global,
        MIN(niveau_min) AS niveau_min_absolu,
        MAX(niveau_max) AS niveau_max_absolu,
        STDDEV(niveau_moyen) AS niveau_stddev_global,
        MAX(niveau_max) - MIN(niveau_min) AS amplitude_totale,
        
        -- Profondeur
        AVG(profondeur_moyenne) AS profondeur_moyenne_globale,
        
        -- Météo
        AVG(temperature_moyenne) AS temperature_moyenne_globale,
        AVG(precipitation_totale) AS precipitation_moyenne_mensuelle
        
    FROM {{ ref('fct_monthly_chroniques') }}
    {% if is_incremental() %}
    WHERE code_bss IN (SELECT code_bss FROM recent_stations)
    {% endif %}
    GROUP BY code_bss
),

-- Tendance sur les 5 dernières années (régression linéaire)
tendance_recente AS (
    SELECT
        code_bss,
        
        -- Régression linéaire: niveau = a * temps + b
        -- slope > 0 = hausse, slope < 0 = baisse
        REGR_SLOPE(niveau_moyen, EXTRACT(EPOCH FROM mois)) AS slope_niveau,
        REGR_INTERCEPT(niveau_moyen, EXTRACT(EPOCH FROM mois)) AS intercept_niveau,
        REGR_R2(niveau_moyen, EXTRACT(EPOCH FROM mois)) AS r2_niveau,
        
        -- Tendance précipitations
        REGR_SLOPE(precipitation_totale, EXTRACT(EPOCH FROM mois)) AS slope_precipitation,
        
        COUNT(*) AS nb_mois_tendance
        
    FROM {{ ref('fct_monthly_chroniques') }}
    WHERE mois >= CURRENT_DATE - INTERVAL '5 years'
    {% if is_incremental() %}
      AND code_bss IN (SELECT code_bss FROM recent_stations)
    {% endif %}
    GROUP BY code_bss
    HAVING COUNT(*) >= 24  -- Au moins 2 ans de données
),

-- Dernières valeurs connues
derniere_annee AS (
    SELECT DISTINCT ON (code_bss)
        code_bss,
        annee AS derniere_annee,
        niveau_moyen_annuel AS niveau_derniere_annee,
        classification_niveau_annuel AS classification_derniere_annee,
        percentile_niveau_historique AS percentile_derniere_annee
    FROM {{ ref('fct_yearly_stats') }}
    {% if is_incremental() %}
    WHERE code_bss IN (SELECT code_bss FROM recent_stations)
    {% endif %}
    ORDER BY code_bss, annee DESC
)

SELECT
    -- Identifiants et métadonnées de base
    s.code_bss,
    s.bss_id,
    s.nom_commune,
    s.code_commune_insee,
    s.code_departement,
    s.nom_departement,
    s.codes_bdlisa,
    s.altitude_station,
    s.x AS longitude,
    s.y AS latitude,
    s.geometry AS geom,
    
    -- Période de mesure
    s.date_debut_mesure,
    s.date_fin_mesure,
    s.nb_mesures_piezo AS nb_mesures_source,
    sg.nb_mesures_total,
    sg.nb_mois_total,
    rd.premiere_mesure,
    rd.derniere_mesure,
    
    -- Statistiques piézométriques globales
    sg.niveau_moyen_global,
    sg.niveau_min_absolu,
    sg.niveau_max_absolu,
    sg.niveau_stddev_global,
    sg.amplitude_totale,
    sg.profondeur_moyenne_globale,
    
    -- Statistiques météo
    sg.temperature_moyenne_globale,
    sg.precipitation_moyenne_mensuelle,
    
    -- Dernière année connue
    da.derniere_annee,
    da.niveau_derniere_annee,
    da.classification_derniere_annee,
    da.percentile_derniere_annee,
    
    -- Tendance (régression sur 5 ans)
    t.slope_niveau,
    t.r2_niveau,
    t.slope_precipitation,
    t.nb_mois_tendance,
    
    -- Classification de la tendance
    CASE 
        WHEN t.slope_niveau IS NULL THEN 'INSUFFISANT'
        WHEN t.slope_niveau > 0.0001 THEN 'HAUSSE'
        WHEN t.slope_niveau < -0.0001 THEN 'BAISSE'
        ELSE 'STABLE'
    END AS tendance_classification,
    
    -- Niveau d'alerte
    CASE 
        WHEN t.slope_niveau < -0.0005 THEN 'ALERTE'
        WHEN t.slope_niveau < -0.0001 THEN 'VIGILANCE'
        ELSE 'NORMAL'
    END AS niveau_alerte,
    
    -- Qualité de la tendance (R² > 0.5 = fiable)
    CASE 
        WHEN t.r2_niveau IS NULL THEN 'NON_CALCULEE'
        WHEN t.r2_niveau >= 0.5 THEN 'FIABLE'
        WHEN t.r2_niveau >= 0.2 THEN 'INDICATIVE'
        ELSE 'FAIBLE'
    END AS qualite_tendance,
    
    -- Métadonnées techniques
    CURRENT_TIMESTAMP AS date_calcul

FROM stations s
LEFT JOIN real_dates rd ON s.code_bss = rd.code_bss
LEFT JOIN stats_globales sg ON s.code_bss = sg.code_bss
LEFT JOIN tendance_recente t ON s.code_bss = t.code_bss
LEFT JOIN derniere_annee da ON s.code_bss = da.code_bss
