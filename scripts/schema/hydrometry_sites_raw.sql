-- Table: hydrometry_sites_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.hydrometry_sites_raw (
    altitude_site DOUBLE PRECISION,
    code_commune_site TEXT,
    code_cours_eau TEXT,
    code_departement TEXT,
    code_entite_hydro_site TEXT,
    code_projection TEXT,
    code_region TEXT,
    code_site TEXT,
    code_systeme_alti_site TEXT,
    code_troncon_hydro_site TEXT,
    code_zone_hydro_site TEXT,
    commentaire_influence_generale_site TEXT,
    commentaire_site TEXT,
    coordonnee_x_site DOUBLE PRECISION,
    coordonnee_y_site DOUBLE PRECISION,
    date_maj_site TIMESTAMP,
    date_premiere_donnee_dispo_site TEXT,
    grandeur_hydro TEXT,
    influence_generale_site BIGINT,
    latitude_site DOUBLE PRECISION,
    libelle_commune TEXT,
    libelle_cours_eau TEXT,
    libelle_departement TEXT,
    libelle_region TEXT,
    libelle_site TEXT,
    longitude_site DOUBLE PRECISION,
    premier_mois_annee_hydro_site BIGINT,
    premier_mois_etiage_site BIGINT,
    statut_site BIGINT,
    surface_bv DOUBLE PRECISION,
    type_contexte_loi_stat_site TEXT,
    type_loi_site TEXT,
    type_site TEXT,
    uri_cours_eau TEXT,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for site-based queries
CREATE INDEX IF NOT EXISTS idx_hydrometry_sites_raw_code_site
ON hubeau.hydrometry_sites_raw(code_site);

-- Index for date updates
CREATE INDEX IF NOT EXISTS idx_hydrometry_sites_raw_date_maj_site
ON hubeau.hydrometry_sites_raw(date_maj_site);

COMMENT ON TABLE hubeau.hydrometry_sites_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
