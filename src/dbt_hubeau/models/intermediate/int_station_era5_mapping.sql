{{
  config(
    materialized = 'table',
    indexes=[
      {'columns': ['code_bss'], 'unique': True},
      {'columns': ['era5_latitude', 'era5_longitude']},
      {'columns': ['geom'], 'type': 'gist'}
    ],
    post_hook=[
      "{{ add_primary_key(['code_bss']) }}",
      "{{ add_foreign_key(['code_bss'], 'stg_piezo_stations', ['code_bss']) }}"
    ]
  )
}}

-- Mapping spatial: Stations piézo → Point de grille ERA5 le plus proche
-- Utilise PostGIS KNN (opérateur <->) pour trouver le vrai nearest neighbor
-- TME (libellés BDLISA): jointure par code (TRIM) → fallback par n'importe quel code dans codes_bdlisa → fallback spatial (ST_Contains).
-- Materialized = table pour rebuild complet à chaque run (libellés TME à jour).

WITH stations AS (
    SELECT * FROM {{ ref('stg_piezo_stations') }}
    WHERE geometry IS NOT NULL
      -- Filtre pour la France métropolitaine (Hexagone) uniquement
      -- Les DOM-TOM n'ont pas de couverture ERA5 dans ce dataset
      AND y >= 41.0 AND y <= 51.5 
      AND x >= -5.5 AND x <= 10.0
),

era5_grid AS (
    SELECT * FROM {{ ref('int_era5_grid_points') }}
),

tme AS (
    SELECT * FROM {{ ref('stg_tme_entites') }}
),

-- PostGIS KNN: Find nearest ERA5 grid point for each station
station_nearest_era5 AS (
    SELECT DISTINCT ON (s.code_bss)
        s.code_bss,
        s.y AS station_latitude,
        s.x AS station_longitude,
        s.geometry AS station_geom,
        s.codes_bdlisa,
        NULLIF(TRIM(SPLIT_PART(COALESCE(s.codes_bdlisa, ''), ',', 1)), '') AS code_eh_primary,
        s.code_commune_insee,
        s.nom_commune,
        s.altitude_station,
        s.code_departement,
        s.nom_departement,
        -- ERA5 grid point info
        e.era5_latitude,
        e.era5_longitude,
        e.geom AS era5_geom,
        -- Distance in meters (using geography for accuracy)
        ST_Distance(s.geometry::geography, e.geom::geography) AS distance_m
    FROM stations s
    CROSS JOIN LATERAL (
        SELECT era5_latitude, era5_longitude, geom
        FROM era5_grid
        ORDER BY s.geometry <-> geom  -- KNN operator (uses GiST index)
        LIMIT 1
    ) e
),

-- TME par code principal (jointure normalisée TRIM)
tme_by_code AS (
    SELECT sne.*, t.code_eh AS t_code_eh, t.libelle_eh, t.niveau_eh, t.libelle_niveau_eh,
           t.etat_eh, t.libelle_etat_eh, t.nature_eh, t.libelle_nature_eh,
           t.milieu_eh, t.libelle_milieu_eh, t.theme_eh, t.libelle_theme_eh,
           t.origine_eh, t.libelle_origine_eh
    FROM station_nearest_era5 sne
    LEFT JOIN tme t ON sne.code_eh_primary IS NOT NULL
      AND TRIM(t.code_eh) = sne.code_eh_primary
),

-- Fallback: TME par n'importe quel code dans codes_bdlisa (si jointure code principal vide)
tme_by_any_code AS (
    SELECT tc.*,
           t2.code_eh AS t_any_eh, t2.niveau_eh AS niv_any, t2.libelle_eh AS l_any, t2.libelle_niveau_eh AS ln_any,
           t2.etat_eh AS e_any, t2.libelle_etat_eh AS le_any, t2.nature_eh AS n_any, t2.libelle_nature_eh AS lnat_any,
           t2.milieu_eh AS m_any, t2.libelle_milieu_eh AS lm_any, t2.theme_eh AS th_any, t2.libelle_theme_eh AS lth_any,
           t2.origine_eh AS o_any, t2.libelle_origine_eh AS lo_any
    FROM tme_by_code tc
    LEFT JOIN LATERAL (
        SELECT * FROM tme t2
        WHERE TRIM(t2.code_eh) = ANY(
            ARRAY(SELECT TRIM(c) FROM unnest(STRING_TO_ARRAY(COALESCE(tc.codes_bdlisa, ''), ',')) AS c WHERE TRIM(c) != '')
        )
        LIMIT 1
    ) t2 ON tc.t_code_eh IS NULL
)

SELECT
    tac.code_bss::text AS code_bss,
    tac.era5_latitude::numeric AS era5_latitude,
    tac.era5_longitude::numeric AS era5_longitude,
    tac.station_latitude::numeric AS station_latitude,
    tac.station_longitude::numeric AS station_longitude,
    tac.station_geom AS geom,
    (tac.distance_m)::numeric AS era5_distance_m,
    tac.codes_bdlisa::text AS codes_bdlisa,
    tac.code_commune_insee::text AS code_commune_insee,
    tac.nom_commune::text AS nom_commune,
    tac.altitude_station::numeric AS altitude_station,
    tac.code_departement::text AS code_departement,
    tac.nom_departement::text AS nom_departement,
    COALESCE(tac.t_code_eh, tac.t_any_eh, tgeo.code_eh, tac.code_eh_primary)::text AS code_eh,
    COALESCE(tac.libelle_eh, tac.l_any, tgeo.libelle_eh)::text AS libelle_eh,
    COALESCE(tac.niveau_eh, tac.niv_any, tgeo.niveau_eh)::text AS niveau_eh,
    COALESCE(tac.libelle_niveau_eh, tac.ln_any, tgeo.libelle_niveau_eh)::text AS libelle_niveau_eh,
    COALESCE(tac.etat_eh, tac.e_any, tgeo.etat_eh)::text AS etat_eh,
    COALESCE(tac.libelle_etat_eh, tac.le_any, tgeo.libelle_etat_eh)::text AS libelle_etat_eh,
    COALESCE(tac.nature_eh, tac.n_any, tgeo.nature_eh)::text AS nature_eh,
    COALESCE(tac.libelle_nature_eh, tac.lnat_any, tgeo.libelle_nature_eh)::text AS libelle_nature_eh,
    COALESCE(tac.milieu_eh, tac.m_any, tgeo.milieu_eh)::text AS milieu_eh,
    COALESCE(tac.libelle_milieu_eh, tac.lm_any, tgeo.libelle_milieu_eh)::text AS libelle_milieu_eh,
    COALESCE(tac.theme_eh, tac.th_any, tgeo.theme_eh)::text AS theme_eh,
    COALESCE(tac.libelle_theme_eh, tac.lth_any, tgeo.libelle_theme_eh)::text AS libelle_theme_eh,
    COALESCE(tac.origine_eh, tac.o_any, tgeo.origine_eh)::text AS origine_eh,
    COALESCE(tac.libelle_origine_eh, tac.lo_any, tgeo.libelle_origine_eh)::text AS libelle_origine_eh
FROM tme_by_any_code tac
LEFT JOIN LATERAL (
    SELECT code_eh, libelle_eh, niveau_eh, libelle_niveau_eh, etat_eh, libelle_etat_eh,
           nature_eh, libelle_nature_eh, milieu_eh, libelle_milieu_eh, theme_eh, libelle_theme_eh,
           origine_eh, libelle_origine_eh
    FROM tme t3
    WHERE t3.geometry IS NOT NULL
      AND ST_Contains(t3.geometry, tac.station_geom)
    ORDER BY ST_Area(t3.geometry) ASC
    LIMIT 1
) tgeo ON tac.t_code_eh IS NULL AND tac.t_any_eh IS NULL
