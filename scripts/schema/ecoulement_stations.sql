-- Table: ecoulement_stations
-- Source: Hub'Eau API
-- PRIMARY KEY: code_station

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.ecoulement_stations (
    code_bassin TEXT,
    code_commune TEXT,
    code_cours_eau TEXT,
    code_departement TEXT,
    code_epsg_station TEXT,
    code_projection_station TEXT,
    code_region TEXT,
    code_station BIGINT NOT NULL,
    coordonnee_x_station DOUBLE PRECISION,
    coordonnee_y_station DOUBLE PRECISION,
    date_maj_station TIMESTAMP,
    etat_station TEXT,
    latitude DOUBLE PRECISION,
    libelle_bassin TEXT,
    libelle_commune TEXT,
    libelle_cours_eau TEXT,
    libelle_departement TEXT,
    libelle_projection_station TEXT,
    libelle_region TEXT,
    libelle_station TEXT,
    longitude DOUBLE PRECISION,
    uri_cours_eau TEXT,
    uri_station TEXT,
    PRIMARY KEY (code_station)
);
-- Index temporels
CREATE INDEX IF NOT EXISTS idx_ecoulement_stations_date_maj_station
ON hubeau.ecoulement_stations(date_maj_station);

COMMENT ON TABLE hubeau.ecoulement_stations IS
'Table Hub''Eau: ecoulement_stations - PRIMARY KEY: code_station';
