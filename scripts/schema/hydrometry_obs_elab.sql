-- Table: hydrometry_obs_elab
-- Source: Hub'Eau API
-- PRIMARY KEY: code_site, date_obs_elab

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.hydrometry_obs_elab (
    code_methode TEXT,
    code_qualification TEXT,
    code_site TEXT NOT NULL,
    code_station BIGINT,
    code_statut TEXT,
    date_obs_elab TIMESTAMP NOT NULL,
    date_prod TIMESTAMP,
    grandeur_hydro_elab TEXT,
    latitude DOUBLE PRECISION,
    libelle_methode TEXT,
    libelle_qualification TEXT,
    libelle_statut TEXT,
    longitude DOUBLE PRECISION,
    resultat_obs_elab DOUBLE PRECISION,
    PRIMARY KEY (code_site, date_obs_elab)
);
-- Index temporels
CREATE INDEX IF NOT EXISTS idx_hydrometry_obs_elab_date_obs_elab
ON hubeau.hydrometry_obs_elab(date_obs_elab);

CREATE INDEX IF NOT EXISTS idx_hydrometry_obs_elab_date_prod
ON hubeau.hydrometry_obs_elab(date_prod);

COMMENT ON TABLE hubeau.hydrometry_obs_elab IS
'Table Hub''Eau: hydrometry_obs_elab - PRIMARY KEY: code_site, date_obs_elab';
