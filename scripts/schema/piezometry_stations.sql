-- Table: piezometry_stations
-- Source: Hub'Eau API
-- PRIMARY KEY: code_bss

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.piezometry_stations (
    altitude_station DOUBLE PRECISION,
    bss_id TEXT,
    code_bss TEXT NOT NULL,
    code_commune_insee TEXT,
    code_departement TEXT,
    codes_bdlisa TEXT,
    codes_masse_eau_edl TEXT,
    date_debut_mesure TIMESTAMP,
    date_fin_mesure TIMESTAMP,
    date_maj TIMESTAMP,
    libelle_pe TEXT,
    nb_mesures_piezo BIGINT,
    nom_commune TEXT,
    nom_departement TEXT,
    noms_masse_eau_edl TEXT,
    profondeur_investigation DOUBLE PRECISION,
    urn_bss TEXT,
    urns_bdlisa TEXT,
    urns_masse_eau_edl TEXT,
    x DOUBLE PRECISION,
    y DOUBLE PRECISION,
    PRIMARY KEY (code_bss)
);
-- Index temporels
CREATE INDEX IF NOT EXISTS idx_piezometry_stations_date_debut_mesure
ON hubeau.piezometry_stations(date_debut_mesure);

CREATE INDEX IF NOT EXISTS idx_piezometry_stations_date_fin_mesure
ON hubeau.piezometry_stations(date_fin_mesure);

CREATE INDEX IF NOT EXISTS idx_piezometry_stations_date_maj
ON hubeau.piezometry_stations(date_maj);

COMMENT ON TABLE hubeau.piezometry_stations IS
'Table Hub''Eau: piezometry_stations - PRIMARY KEY: code_bss';
