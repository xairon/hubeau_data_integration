-- Table: hydrometry_stations_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.hydrometry_stations_raw (
    altitude_ref_alti_station DOUBLE PRECISION,
    code_commune_station TEXT,
    code_cours_eau TEXT,
    code_departement TEXT,
    code_finalite_station TEXT,
    code_projection TEXT,
    code_regime_station TEXT,
    code_region TEXT,
    code_sandre_reseau_station TEXT,
    code_site TEXT,
    code_station BIGINT,
    code_systeme_alti_site TEXT,
    commentaire_influence_locale_station TEXT,
    commentaire_station TEXT,
    coordlatlon TEXT,
    coordonnee_x_station DOUBLE PRECISION,
    coordonnee_y_station DOUBLE PRECISION,
    date_activation_ref_alti_station TIMESTAMP,
    date_debut_ref_alti_station TIMESTAMP,
    date_fermeture_station TIMESTAMP,
    date_maj_ref_alti_station TIMESTAMP,
    date_maj_station TIMESTAMP,
    date_ouverture_station TIMESTAMP,
    descriptif_station TEXT,
    en_service BOOLEAN,
    influence_locale_station TEXT,
    latitude_station DOUBLE PRECISION,
    libelle_commune TEXT,
    libelle_cours_eau TEXT,
    libelle_departement TEXT,
    libelle_region TEXT,
    libelle_site TEXT,
    libelle_station TEXT,
    longitude_station DOUBLE PRECISION,
    qualification_donnees_station BIGINT,
    type_contexte_loi_stat_station TEXT,
    type_loi_station TEXT,
    type_station TEXT,
    uri_cours_eau TEXT,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for station-based queries
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_raw_code_station
ON hubeau.hydrometry_stations_raw(code_station);

-- Index for date updates
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_raw_date_maj_station
ON hubeau.hydrometry_stations_raw(date_maj_station);

COMMENT ON TABLE hubeau.hydrometry_stations_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
