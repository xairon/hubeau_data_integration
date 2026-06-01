{{
  config(
    materialized = 'incremental',
    unique_key = ['code_station'],
    incremental_strategy = 'delete+insert',
    indexes = [
      {'columns': ['code_site']},
      {'columns': ['code_departement']},
      {'columns': ['grandeur_hydro_principale']},
      {'columns': ['geometry'], 'type': 'gist'}
    ],
    post_hook=[
      "{{ add_primary_key(['code_station']) }}",
      "{{ add_foreign_key(['code_station'], 'stg_hydrometry_stations', ['code_station']) }}",
      "{{ add_foreign_key(['code_site'], 'stg_hydrometry_sites', ['code_site']) }}"
    ]
  )
}}

-- Dimension stations hydrométriques enrichie
-- Source: stg_hydrometry_stations + fct_monthly_hydro + fct_yearly_hydro
-- INCREMENTAL: recalcul des stations récentes uniquement.

{% set lookback_days = var('streaming_lookback_days', 7) %}

WITH recent_stations AS (
    SELECT DISTINCT code_station
    FROM {{ ref('hydro_daily_chroniques') }}
    WHERE date >= CURRENT_DATE - INTERVAL '{{ lookback_days }} days'
),
stations AS (
    SELECT * FROM {{ ref('stg_hydrometry_stations') }}
    {% if is_incremental() %}
    WHERE code_station IN (SELECT code_station FROM recent_stations)
    {% endif %}
),

-- Vraies dates de mesure (date quotidienne réelle, PAS le bucket mensuel),
-- toutes grandeurs confondues. derniere_mesure = dernier relevé réel (ex. hier).
real_dates AS (
    SELECT
        code_station,
        MIN(date) AS premiere_mesure,
        MAX(date) AS derniere_mesure
    FROM {{ ref('hydro_daily_chroniques') }}
    {% if is_incremental() %}
    WHERE code_station IN (SELECT code_station FROM recent_stations)
    {% endif %}
    GROUP BY code_station
),

monthly_stats AS (
    SELECT
        code_station,
        grandeur_hydro_elab,
        SUM(nb_jours_mesures) AS nb_jours_total,
        COUNT(*) AS nb_mois_total,
        AVG(resultat_moyen) AS resultat_moyen_global,
        MIN(resultat_min) AS resultat_min_global,
        MAX(resultat_max) AS resultat_max_global,
        STDDEV(resultat_moyen) AS resultat_stddev_global
    FROM {{ ref('fct_monthly_hydro') }}
    {% if is_incremental() %}
    WHERE code_station IN (SELECT code_station FROM recent_stations)
    {% endif %}
    GROUP BY code_station, grandeur_hydro_elab
),

primary_grandeur AS (
    SELECT DISTINCT ON (code_station)
        code_station,
        grandeur_hydro_elab
    FROM monthly_stats
    ORDER BY code_station, nb_jours_total DESC NULLS LAST
),

stats_globales AS (
    SELECT
        ms.*
    FROM monthly_stats ms
    INNER JOIN primary_grandeur pg
        ON ms.code_station = pg.code_station
       AND ms.grandeur_hydro_elab = pg.grandeur_hydro_elab
),

derniere_annee AS (
    SELECT DISTINCT ON (code_station)
        fy.code_station,
        fy.grandeur_hydro_elab,
        fy.annee AS annee_dernier_bilan,
        fy.resultat_moyen_annuel AS resultat_moyen_dern_annee,
        fy.classification_resultat_annuel AS classification_resultat_dern_annee,
        fy.percentile_resultat_historique AS percentile_resultat_dern_annee
    FROM {{ ref('fct_yearly_hydro') }} fy
    INNER JOIN primary_grandeur pg
        ON fy.code_station = pg.code_station
       AND fy.grandeur_hydro_elab = pg.grandeur_hydro_elab
    {% if is_incremental() %}
    WHERE fy.code_station IN (SELECT code_station FROM recent_stations)
    {% endif %}
    ORDER BY fy.code_station, fy.annee DESC
),

metadata AS (
    SELECT
        code_station,
        libelle_station,
        code_site,
        libelle_site,
        code_cours_eau,
        -- Hub'Eau fournit libelle_cours_eau / uri_cours_eau (pas "nom_cours_eau")
        libelle_cours_eau AS nom_cours_eau,
        uri_cours_eau,
        code_departement,
        -- Hub'Eau fournit libelle_departement (pas "nom_departement")
        libelle_departement AS nom_departement,
        date_ouverture_station,
        date_fermeture_station,
        longitude_station,
        latitude_station,
        geometry,
        
        -- Statut
        CASE 
            WHEN date_fermeture_station IS NULL OR date_fermeture_station > CURRENT_DATE THEN 'ACTIVE'
            ELSE 'FERMEE'
        END AS statut_station
        
    FROM stations
)

SELECT
    m.*,
    pg.grandeur_hydro_elab AS grandeur_hydro_principale,
    rd.premiere_mesure,
    rd.derniere_mesure,
    sg.nb_jours_total,
    sg.nb_mois_total,
    sg.resultat_moyen_global,
    sg.resultat_min_global,
    sg.resultat_max_global,
    sg.resultat_stddev_global,
    da.annee_dernier_bilan,
    da.resultat_moyen_dern_annee,
    da.classification_resultat_dern_annee,
    da.percentile_resultat_dern_annee
FROM metadata m
LEFT JOIN real_dates rd ON m.code_station = rd.code_station
LEFT JOIN primary_grandeur pg ON m.code_station = pg.code_station
LEFT JOIN stats_globales sg
    ON m.code_station = sg.code_station
   AND pg.grandeur_hydro_elab = sg.grandeur_hydro_elab
LEFT JOIN derniere_annee da
    ON m.code_station = da.code_station
   AND pg.grandeur_hydro_elab = da.grandeur_hydro_elab
