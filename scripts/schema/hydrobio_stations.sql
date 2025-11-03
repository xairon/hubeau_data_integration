-- Table: hydrobio_stations
-- Source: Hub'Eau API
-- PRIMARY KEY: code_station_hydrobio

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.hydrobio_stations (
    code_bassin TEXT,
    code_commune TEXT,
    code_cours_eau TEXT,
    code_departement TEXT,
    code_masse_eau TEXT,
    code_projection TEXT,
    code_region TEXT,
    code_sous_bassin TEXT,
    code_station_hydrobio TEXT NOT NULL,
    codes_appel_taxons TEXT,
    codes_indices TEXT,
    codes_reseaux TEXT,
    codes_supports TEXT,
    coordonnee_x DOUBLE PRECISION,
    coordonnee_y DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    libelle_bassin TEXT,
    libelle_commune TEXT,
    libelle_cours_eau TEXT,
    libelle_departement TEXT,
    libelle_masse_eau TEXT,
    libelle_region TEXT,
    libelle_sous_bassin TEXT,
    libelle_station_hydrobio TEXT,
    libelles_appel_taxons TEXT,
    libelles_indices TEXT,
    libelles_reseaux TEXT,
    libelles_supports TEXT,
    longitude DOUBLE PRECISION,
    uri_cours_eau TEXT,
    uri_masse_eau TEXT,
    uri_station_hydrobio TEXT,
    PRIMARY KEY (code_station_hydrobio)
);
COMMENT ON TABLE hubeau.hydrobio_stations IS
'Table Hub''Eau: hydrobio_stations - PRIMARY KEY: code_station_hydrobio';
