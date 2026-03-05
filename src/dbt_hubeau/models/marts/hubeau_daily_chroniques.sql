{{
  config(
    materialized = 'incremental',
    unique_key = ['code_bss', 'date'],
    incremental_strategy = 'append',
    indexes = [
      {'columns': ['code_bss']},
      {'columns': ['date'], 'type': 'brin'},
      {'columns': ['code_departement']},
      {'columns': ['code_eh']}
    ],
    pre_hook = [
      "{{ hypertable_delete('date', '30 days') }}"
    ],
    post_hook = [
      "{{ add_primary_key(['code_bss', 'date']) }}",
      "{{ convert_to_hypertable('date', '1 year') }}",
      "{{ enable_compression(segment_by=['code_bss'], order_by='date DESC', compress_after='365 days') }}"
    ]
  )
}}

-- Table finale : Piézométrie + ERA5 + TME.
-- INCREMENTAL: pre-hook DELETE + append (pas de DELETE...USING qui scanne tous les chunks).
-- La fenêtre de re-calcul est synchronisée avec le pre-hook hypertable_delete.

WITH measurements AS (
    SELECT * FROM {{ ref('int_daily_measurements') }}
    {% if is_incremental() %}
    WHERE date_mesure >= CURRENT_DATE - INTERVAL '30 days'
    {% endif %}
),

mapping AS (
    SELECT * FROM {{ ref('int_station_era5_mapping') }}
),

era5_filtered AS (
    SELECT * FROM {{ ref('int_era5_for_stations') }}
    {% if is_incremental() %}
    WHERE era5_date >= CURRENT_DATE - INTERVAL '30 days'
    {% endif %}
),

final AS (
    SELECT
        m.code_bss::text AS code_bss,
        m.date_mesure::date AS date,

        m.niveau_nappe_eau::numeric AS niveau_nappe_eau,
        m.profondeur_nappe::numeric AS profondeur_nappe,

        e.temperature_2m::numeric AS temperature_2m,
        e.total_precipitation::numeric AS total_precipitation,
        e.potential_evaporation::numeric AS potential_evaporation,

        map.codes_bdlisa::text AS codes_bdlisa,
        map.code_commune_insee::text AS code_commune_insee,
        map.nom_commune::text AS nom_commune,
        map.altitude_station::numeric AS altitude_station,
        map.code_departement::text AS code_departement,
        map.nom_departement::text AS nom_departement,

        map.code_eh::text AS code_eh,
        map.libelle_eh::text AS libelle_eh,
        map.niveau_eh::text AS niveau_eh,
        map.etat_eh::text AS etat_eh,
        map.nature_eh::text AS nature_eh,
        map.milieu_eh::text AS milieu_eh,
        map.theme_eh::text AS theme_eh,
        map.origine_eh::text AS origine_eh,

        map.station_latitude::numeric AS station_latitude,
        map.station_longitude::numeric AS station_longitude,
        map.era5_latitude::numeric AS era5_latitude,
        map.era5_longitude::numeric AS era5_longitude

    FROM measurements m
    LEFT JOIN mapping map ON m.code_bss = map.code_bss
    LEFT JOIN era5_filtered e
        ON map.era5_latitude = e.latitude
        AND map.era5_longitude = e.longitude
        AND m.date_mesure = e.era5_date
)

SELECT * FROM final
