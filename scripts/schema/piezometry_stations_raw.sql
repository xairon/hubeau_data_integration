-- Table: piezometry_stations_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.piezometry_stations_raw (
    altitude_station DOUBLE PRECISION,
    bss_id TEXT,
    code_bss TEXT,
    code_commune_insee TEXT,
    code_departement TEXT,
    codes_bdlisa TEXT,
    codes_masse_eau_edl TEXT,
    date_debut_mesure TIMESTAMP,
    date_fin_mesure TIMESTAMP,
    date_maj TIMESTAMP,
    libelle_pe TEXT,
    nb_mesures_piezo BIGINT,
    nom_commune TEXT,
    nom_departement TEXT,
    noms_masse_eau_edl TEXT,
    profondeur_investigation DOUBLE PRECISION,
    urn_bss TEXT,
    urns_bdlisa TEXT,
    urns_masse_eau_edl TEXT,
    x DOUBLE PRECISION,
    y DOUBLE PRECISION,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for code_bss queries
CREATE INDEX IF NOT EXISTS idx_piezometry_stations_raw_code_bss
ON hubeau.piezometry_stations_raw(code_bss);

-- Index for date updates
CREATE INDEX IF NOT EXISTS idx_piezometry_stations_raw_date_maj
ON hubeau.piezometry_stations_raw(date_maj);

COMMENT ON TABLE hubeau.piezometry_stations_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
