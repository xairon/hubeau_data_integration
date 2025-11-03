-- Table: ecoulement_observations
-- Source: Hub'Eau API
-- PRIMARY KEY: code_station, date_observation

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.ecoulement_observations (
    code_bassin TEXT,
    code_campagne TEXT,
    code_commune TEXT,
    code_cours_eau TEXT,
    code_departement TEXT,
    code_ecoulement TEXT,
    code_projection_station TEXT,
    code_region TEXT,
    code_reseau TEXT,
    code_station BIGINT NOT NULL,
    coordonnee_x_station DOUBLE PRECISION,
    coordonnee_y_station DOUBLE PRECISION,
    date_observation TIMESTAMP NOT NULL,
    latitude DOUBLE PRECISION,
    libelle_bassin TEXT,
    libelle_commune TEXT,
    libelle_cours_eau TEXT,
    libelle_departement TEXT,
    libelle_ecoulement TEXT,
    libelle_projection_station TEXT,
    libelle_region TEXT,
    libelle_reseau TEXT,
    libelle_station TEXT,
    longitude DOUBLE PRECISION,
    uri_cours_eau TEXT,
    uri_reseau TEXT,
    uri_station TEXT,
    PRIMARY KEY (code_station, date_observation)
);
-- Index temporels
CREATE INDEX IF NOT EXISTS idx_ecoulement_observations_date_observation
ON hubeau.ecoulement_observations(date_observation);

COMMENT ON TABLE hubeau.ecoulement_observations IS
'Table Hub''Eau: ecoulement_observations - PRIMARY KEY: code_station, date_observation';
