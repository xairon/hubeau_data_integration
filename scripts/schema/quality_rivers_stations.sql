-- Table: quality_rivers_stations
-- Source: Hub'Eau API
-- PRIMARY KEY: code_station

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.quality_rivers_stations (
    altitude_point_caracteristique DOUBLE PRECISION,
    code_bassin TEXT,
    code_commune TEXT,
    code_cours_eau TEXT,
    code_departement TEXT,
    code_eu_bassin TEXT,
    code_eu_masse_deau TEXT,
    code_eu_sous_bassin TEXT,
    code_masse_deau TEXT,
    code_projection TEXT,
    code_region TEXT,
    code_station BIGINT NOT NULL,
    commentaire TEXT,
    coordonnee_x DOUBLE PRECISION,
    coordonnee_y DOUBLE PRECISION,
    date_arret TEXT,
    date_creation TIMESTAMP,
    date_maj_information TIMESTAMP,
    durete DOUBLE PRECISION,
    finalite TEXT,
    latitude DOUBLE PRECISION,
    libelle_commune TEXT,
    libelle_departement TEXT,
    libelle_projection TEXT,
    libelle_region TEXT,
    libelle_station TEXT,
    localisation_precise TEXT,
    longitude DOUBLE PRECISION,
    nature TEXT,
    nom_bassin TEXT,
    nom_cours_eau TEXT,
    nom_masse_deau TEXT,
    nom_sous_bassin TEXT,
    point_kilometrique DOUBLE PRECISION,
    premier_mois_annee_etiage DOUBLE PRECISION,
    superficie_bassin_versant_reel DOUBLE PRECISION,
    superficie_bassin_versant_topo DOUBLE PRECISION,
    type_entite_hydro BIGINT,
    uri_bassin TEXT,
    uri_cours_eau TEXT,
    uri_masse_deau TEXT,
    uri_sous_bassin TEXT,
    uri_station TEXT,
    PRIMARY KEY (code_station)
);
-- Index temporels
CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_date_creation
ON hubeau.quality_rivers_stations(date_creation);

CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_date_maj_information
ON hubeau.quality_rivers_stations(date_maj_information);

COMMENT ON TABLE hubeau.quality_rivers_stations IS
'Table Hub''Eau: quality_rivers_stations - PRIMARY KEY: code_station';
