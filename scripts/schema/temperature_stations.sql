-- Table: temperature_stations
-- Source: Hub'Eau API
-- PRIMARY KEY: code_station

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.temperature_stations (
    altitude DOUBLE PRECISION,
    code_bassin TEXT,
    code_commune TEXT,
    code_cours_eau TEXT,
    code_departement TEXT,
    code_masse_eau TEXT,
    code_region TEXT,
    code_sous_bassin TEXT,
    code_station BIGINT NOT NULL,
    code_troncon_hydro TEXT,
    code_type_projection TEXT,
    coordonnee_x DOUBLE PRECISION,
    coordonnee_y DOUBLE PRECISION,
    date_maj_infos TIMESTAMP,
    latitude DOUBLE PRECISION,
    libelle_bassin TEXT,
    libelle_commune TEXT,
    libelle_cours_eau TEXT,
    libelle_departement TEXT,
    libelle_masse_eau TEXT,
    libelle_region TEXT,
    libelle_sous_bassin TEXT,
    libelle_station TEXT,
    localisation TEXT,
    longitude DOUBLE PRECISION,
    pk DOUBLE PRECISION,
    uri_bassin TEXT,
    uri_cours_eau TEXT,
    uri_masse_eau TEXT,
    uri_station TEXT,
    PRIMARY KEY (code_station)
);
-- Index temporels
CREATE INDEX IF NOT EXISTS idx_temperature_stations_date_maj_infos
ON hubeau.temperature_stations(date_maj_infos);

COMMENT ON TABLE hubeau.temperature_stations IS
'Table Hub''Eau: temperature_stations - PRIMARY KEY: code_station';
