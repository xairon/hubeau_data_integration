{{
  config(
    materialized = 'incremental',
    unique_key = ['code_bss', 'mois'],
    incremental_strategy = 'delete+insert',
    indexes = [
      {'columns': ['code_bss']},
      {'columns': ['mois'], 'type': 'brin'},
      {'columns': ['code_departement']}
    ],
    post_hook = [
      "{{ add_primary_key(['code_bss', 'mois']) }}",
      "{{ convert_to_hypertable('mois', '5 years') }}",
      "{{ enable_compression(segment_by=['code_departement'], order_by='mois DESC', compress_after='730 days') }}",
      "{{ add_foreign_key(['code_bss'], 'stg_piezo_stations', ['code_bss']) }}"
    ]
  )
}}

-- Agrégation mensuelle des chroniques piézométriques
-- Source: hubeau_daily_chroniques (fact table quotidienne)
-- Calcule stats mensuelles + moyennes mobiles pour analyse tendancielle
-- INCREMENTAL: recalcul partiel sur une fenêtre mensuelle pour le streaming daily.

{% set months_lookback = var('streaming_monthly_lookback_months', 18) %}

WITH daily AS (
    SELECT * FROM {{ ref('hubeau_daily_chroniques') }}
    {% if is_incremental() %}
    WHERE date >= (
        SELECT COALESCE(MAX(mois), '1900-01-01'::date)
               - INTERVAL '{{ months_lookback }} months'
        FROM {{ this }}
    )
    {% endif %}
),

monthly_agg AS (
    SELECT
        code_bss,
        DATE_TRUNC('month', date)::date AS mois,
        
        -- Métadonnées station (premier du groupe)
        MIN(code_departement) AS code_departement,
        MIN(nom_departement) AS nom_departement,
        MIN(code_eh) AS code_eh,
        MIN(libelle_eh) AS libelle_eh,
        
        -- Piézométrie - Stats mensuelles
        AVG(niveau_nappe_eau) AS niveau_moyen,
        MIN(niveau_nappe_eau) AS niveau_min,
        MAX(niveau_nappe_eau) AS niveau_max,
        MAX(niveau_nappe_eau) - MIN(niveau_nappe_eau) AS amplitude_mensuelle,
        STDDEV(niveau_nappe_eau) AS niveau_stddev,
        
        -- Profondeur nappe
        AVG(profondeur_nappe) AS profondeur_moyenne,
        MIN(profondeur_nappe) AS profondeur_min,
        MAX(profondeur_nappe) AS profondeur_max,
        
        -- Météo ERA5 - Stats mensuelles
        AVG(temperature_2m) AS temperature_moyenne,
        MIN(temperature_2m) AS temperature_min,
        MAX(temperature_2m) AS temperature_max,
        SUM(total_precipitation) AS precipitation_totale,
        AVG(potential_evaporation) AS evaporation_moyenne,
        
        -- Comptage
        COUNT(*) AS nb_jours_mesures,
        
        -- Coordonnées (pour jointures spatiales)
        MIN(era5_latitude) AS era5_latitude,
        MIN(era5_longitude) AS era5_longitude
        
    FROM daily
    GROUP BY code_bss, DATE_TRUNC('month', date)
)

SELECT
    *,
    
    -- Moyennes mobiles (fenêtres glissantes)
    AVG(niveau_moyen) OVER (
        PARTITION BY code_bss 
        ORDER BY mois 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS niveau_moy_mobile_3m,
    
    AVG(niveau_moyen) OVER (
        PARTITION BY code_bss 
        ORDER BY mois 
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS niveau_moy_mobile_12m,
    
    AVG(precipitation_totale) OVER (
        PARTITION BY code_bss 
        ORDER BY mois 
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS precipitation_moy_mobile_12m,
    
    -- Variation vs mois précédent
    niveau_moyen - LAG(niveau_moyen) OVER (
        PARTITION BY code_bss ORDER BY mois
    ) AS variation_niveau_vs_mois_prec,
    
    -- Variation vs même mois année précédente (saisonnalité)
    niveau_moyen - LAG(niveau_moyen, 12) OVER (
        PARTITION BY code_bss ORDER BY mois
    ) AS variation_niveau_vs_annee_prec
    
FROM monthly_agg
