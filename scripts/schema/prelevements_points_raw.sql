-- Table: prelevements_points_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.prelevements_points_raw (
    code_commune_insee TEXT,
    code_departement TEXT,
    code_entite_hydro_cours_eau TEXT,
    code_entite_hydro_plan_eau TEXT,
    code_mer_ocean TEXT,
    code_nature TEXT,
    code_ouvrage TEXT,
    code_point_prelevement TEXT,
    code_type_milieu TEXT,
    code_zone_hydro TEXT,
    commentaire TEXT,
    date_exploitation_debut TIMESTAMP,
    date_exploitation_fin TEXT,
    libelle_departement TEXT,
    libelle_nature TEXT,
    libelle_type_milieu TEXT,
    lieu_dit TEXT,
    nappe_accompagnement BOOLEAN,
    nom_commune TEXT,
    nom_point_prelevement TEXT,
    uri_bss_point_eau TEXT,
    uri_entite_hydro_cours_eau TEXT,
    uri_entite_hydro_plan_eau TEXT,
    uri_ouvrage TEXT,
    uri_zone_hydro TEXT,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for point-based queries
CREATE INDEX IF NOT EXISTS idx_prelevements_points_raw_code_point_prelevement
ON hubeau.prelevements_points_raw(code_point_prelevement);

-- Index for date updates
CREATE INDEX IF NOT EXISTS idx_prelevements_points_raw_date_exploitation_debut
ON hubeau.prelevements_points_raw(date_exploitation_debut);

COMMENT ON TABLE hubeau.prelevements_points_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
