{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    indexes = [
      {'columns': ['code_station']},
      {'columns': ['code_site']},
      {'columns': ['date'], 'type': 'brin'},
      {'columns': ['grandeur_hydro_elab']},
      {'columns': ['code_departement']}
    ],
    pre_hook = [
      "{{ hypertable_delete('date', '30 days') }}"
    ],
    post_hook = [
      "{{ add_primary_key(['code_station', 'date', 'grandeur_hydro_elab']) }}",
      "{{ convert_to_hypertable('date', '1 year') }}",
      "{{ enable_compression(segment_by=['code_station'], order_by='date DESC', compress_after='365 days') }}"
    ]
  )
}}

-- Table finale : Hydrométrie + ERA5 (sans artefact DLT, types explicites)
-- Filtrage des valeurs nulles : fait en silver (stg_hydrometry_obs_elab, stg_era5_timeseries)
-- INCREMENTAL: pre-hook DELETE + append (pas de DELETE...USING qui scanne tous les chunks).
-- La fenêtre de re-calcul est synchronisée avec le pre-hook hypertable_delete.

WITH measurements AS (
    SELECT * FROM {{ ref('int_hydro_daily_measurements') }}
    {% if is_incremental() %}
    WHERE date_obs_elab >= CURRENT_DATE - INTERVAL '30 days'
    {% endif %}
),

mapping AS (
    SELECT * FROM {{ ref('int_hydro_station_era5_mapping') }}
),

era5_filtered AS (
    SELECT * FROM {{ ref('int_era5_for_all_stations') }}
    {% if is_incremental() %}
    WHERE era5_date >= CURRENT_DATE - INTERVAL '30 days'
    {% endif %}
),

final AS (
    SELECT
        m.code_site::text AS code_site,
        m.code_station::text AS code_station,
        m.date_obs_elab::date AS date,
        m.grandeur_hydro_elab::text AS grandeur_hydro_elab,
        m.resultat_obs_elab::numeric AS resultat_obs_elab,
        m.nb_obs::integer AS nb_obs,

        e.temperature_2m::numeric AS temperature_2m,
        e.total_precipitation::numeric AS total_precipitation,
        e.potential_evaporation::numeric AS potential_evaporation,

        map.code_departement::text AS code_departement,
        map.nom_departement::text AS nom_departement,
        map.code_region::text AS code_region,
        map.libelle_region::text AS libelle_region,
        map.code_commune_site::text AS code_commune_site,
        map.libelle_commune::text AS libelle_commune,

        map.libelle_station::text AS libelle_station,
        map.libelle_site::text AS libelle_site,
        map.type_site::text AS type_site,
        map.surface_bv::numeric AS surface_bv,
        map.code_zone_hydro_site::text AS code_zone_hydro_site,

        map.code_cours_eau::text AS code_cours_eau,
        map.libelle_cours_eau::text AS libelle_cours_eau,
        map.uri_cours_eau::text AS uri_cours_eau,

        map.station_latitude::numeric AS station_latitude,
        map.station_longitude::numeric AS station_longitude,
        map.era5_latitude::numeric AS era5_latitude,
        map.era5_longitude::numeric AS era5_longitude,
        map.era5_distance_m::numeric AS era5_distance_m

    FROM measurements m
    LEFT JOIN mapping map ON m.code_station = map.code_station
    LEFT JOIN era5_filtered e
        ON map.era5_latitude = e.latitude
        AND map.era5_longitude = e.longitude
        AND m.date_obs_elab = e.era5_date
)

SELECT * FROM final
