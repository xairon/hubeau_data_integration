-- Table: quality_groundwater_stations_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.quality_groundwater_stations_raw (
    altitude DOUBLE PRECISION,
    bassin_dce TEXT,
    bss_id TEXT,
    circonscriptions_administrative_bassin TEXT,
    code_bss TEXT,
    code_caracteristique_aquifere TEXT,
    code_etat_pe TEXT,
    code_insee TEXT,
    code_mode_gisement TEXT,
    code_nature_pe TEXT,
    codes_entite_hg_bdlisa TEXT,
    codes_masse_eau_edl TEXT,
    codes_masse_eau_rap TEXT,
    codes_reseau TEXT,
    commentaire_pe TEXT,
    date_debut_mesure TIMESTAMP,
    date_fin_mesure TIMESTAMP,
    latitude DOUBLE PRECISION,
    libelle_pe TEXT,
    longitude DOUBLE PRECISION,
    nom_caracteristique_aquifere TEXT,
    nom_commune TEXT,
    nom_departement TEXT,
    nom_etat_pe TEXT,
    nom_mode_gisement TEXT,
    nom_nature_pe TEXT,
    nom_region TEXT,
    noms_entite_hg_bdlisa TEXT,
    noms_masse_eau_edl TEXT,
    noms_masse_eau_rap TEXT,
    noms_reseau TEXT,
    num_departement TEXT,
    precision_coordonnees DOUBLE PRECISION,
    profondeur_investigation DOUBLE PRECISION,
    uri_caracteristique_aquifere TEXT,
    uri_etat_pe TEXT,
    uri_mode_gisement TEXT,
    uri_nature_pe TEXT,
    uris_reseau TEXT,
    urn_bassin_dce TEXT,
    urn_bss TEXT,
    urns_bdlisa TEXT,
    urns_masse_eau_edl TEXT,
    urns_masse_eau_rap TEXT,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for code_bss queries
CREATE INDEX IF NOT EXISTS idx_quality_groundwater_stations_raw_code_bss
ON hubeau.quality_groundwater_stations_raw(code_bss);

-- Index for date updates
CREATE INDEX IF NOT EXISTS idx_quality_groundwater_stations_raw_date_debut_mesure
ON hubeau.quality_groundwater_stations_raw(date_debut_mesure);

COMMENT ON TABLE hubeau.quality_groundwater_stations_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
