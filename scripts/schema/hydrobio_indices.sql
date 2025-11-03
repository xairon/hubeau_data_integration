-- Table: hydrobio_indices
-- Source: Hub'Eau API
-- PRIMARY KEY: code_station_hydrobio, date_prelevement, code_indice

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.hydrobio_indices (
    code_banque_reference TEXT,
    code_bassin TEXT,
    code_commune TEXT,
    code_cours_eau TEXT,
    code_departement TEXT,
    code_indice TEXT NOT NULL,
    code_masse_eau TEXT,
    code_methode TEXT,
    code_operation_prelevement TEXT,
    code_prelevement TEXT,
    code_projection TEXT,
    code_qualification TEXT,
    code_region TEXT,
    code_sous_bassin TEXT,
    code_station_hydrobio TEXT NOT NULL,
    code_support TEXT,
    coordonnee_x DOUBLE PRECISION,
    coordonnee_y DOUBLE PRECISION,
    date_prelevement TIMESTAMP NOT NULL,
    latitude DOUBLE PRECISION,
    libelle_accreditation TEXT,
    libelle_bassin TEXT,
    libelle_commune TEXT,
    libelle_cours_eau TEXT,
    libelle_departement TEXT,
    libelle_indice TEXT,
    libelle_masse_eau TEXT,
    libelle_methode TEXT,
    libelle_qualification TEXT,
    libelle_region TEXT,
    libelle_sous_bassin TEXT,
    libelle_station_hydrobio TEXT,
    libelle_support TEXT,
    longitude DOUBLE PRECISION,
    resultat_indice DOUBLE PRECISION,
    unite_indice TEXT,
    uri_cours_eau TEXT,
    uri_masse_eau TEXT,
    uri_station_hydrobio TEXT,
    PRIMARY KEY (code_station_hydrobio, date_prelevement, code_indice)
);
-- Index temporels
CREATE INDEX IF NOT EXISTS idx_hydrobio_indices_date_prelevement
ON hubeau.hydrobio_indices(date_prelevement);

COMMENT ON TABLE hubeau.hydrobio_indices IS
'Table Hub''Eau: hydrobio_indices - PRIMARY KEY: code_station_hydrobio, date_prelevement, code_indice';
