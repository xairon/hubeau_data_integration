-- Table: temperature_chroniques
-- Source: Hub'Eau API
-- PRIMARY KEY: code_station, date_mesure_temp, heure_mesure_temp

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.temperature_chroniques (
    code_commune TEXT,
    code_cours_eau TEXT,
    code_parametre BIGINT,
    code_qualification TEXT,
    code_station BIGINT NOT NULL,
    code_unite TEXT,
    date_mesure_temp TIMESTAMP NOT NULL,
    heure_mesure_temp TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    libelle_commune TEXT,
    libelle_cours_eau TEXT,
    libelle_parametre TEXT,
    libelle_qualification TEXT,
    libelle_station TEXT,
    longitude DOUBLE PRECISION,
    resultat DOUBLE PRECISION,
    symbole_unite TEXT,
    uri_cours_eau TEXT,
    uri_station TEXT,
    PRIMARY KEY (code_station, date_mesure_temp, heure_mesure_temp)
);

-- Foreign Key: code_station -> temperature_stations.code_station
ALTER TABLE hubeau.temperature_chroniques DROP CONSTRAINT IF EXISTS fk_temperature_chroniques_code_station;
ALTER TABLE hubeau.temperature_chroniques
ADD CONSTRAINT fk_temperature_chroniques_code_station
FOREIGN KEY (code_station) REFERENCES hubeau.temperature_stations(code_station)
ON DELETE CASCADE;

-- Index temporels
CREATE INDEX IF NOT EXISTS idx_temperature_chroniques_date_mesure_temp
ON hubeau.temperature_chroniques(date_mesure_temp);

COMMENT ON TABLE hubeau.temperature_chroniques IS
'Table Hub''Eau: temperature_chroniques - PRIMARY KEY: code_station, date_mesure_temp, heure_mesure_temp';
