-- Table: hydrometry_stations
-- Source: Hub'Eau API
-- PRIMARY KEY: code_station

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.hydrometry_stations (
    altitude_ref_alti_station DOUBLE PRECISION,
    code_commune_station TEXT,
    code_cours_eau TEXT,
    code_departement TEXT,
    code_finalite_station TEXT,
    code_projection TEXT,
    code_regime_station TEXT,
    code_region TEXT,
    code_sandre_reseau_station TEXT,
    code_site TEXT,
    code_station BIGINT NOT NULL,
    code_systeme_alti_site TEXT,
    commentaire_influence_locale_station TEXT,
    commentaire_station TEXT,
    coordlatlon TEXT,
    coordonnee_x_station DOUBLE PRECISION,
    coordonnee_y_station DOUBLE PRECISION,
    date_activation_ref_alti_station TIMESTAMP,
    date_debut_ref_alti_station TIMESTAMP,
    date_fermeture_station TIMESTAMP,
    date_maj_ref_alti_station TIMESTAMP,
    date_maj_station TIMESTAMP,
    date_ouverture_station TIMESTAMP,
    descriptif_station TEXT,
    en_service BOOLEAN,
    influence_locale_station TEXT,
    latitude_station DOUBLE PRECISION,
    libelle_commune TEXT,
    libelle_cours_eau TEXT,
    libelle_departement TEXT,
    libelle_region TEXT,
    libelle_site TEXT,
    libelle_station TEXT,
    longitude_station DOUBLE PRECISION,
    qualification_donnees_station BIGINT,
    type_contexte_loi_stat_station TEXT,
    type_loi_station TEXT,
    type_station TEXT,
    uri_cours_eau TEXT,
    PRIMARY KEY (code_station)
);

-- Foreign Key: code_site -> hydrometry_sites.code_site
ALTER TABLE hubeau.hydrometry_stations DROP CONSTRAINT IF EXISTS fk_hydrometry_stations_code_site;
ALTER TABLE hubeau.hydrometry_stations
ADD CONSTRAINT fk_hydrometry_stations_code_site
FOREIGN KEY (code_site) REFERENCES hubeau.hydrometry_sites(code_site)
ON DELETE CASCADE;

-- Index temporels
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_date_activation_ref_alti_station
ON hubeau.hydrometry_stations(date_activation_ref_alti_station);

CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_date_debut_ref_alti_station
ON hubeau.hydrometry_stations(date_debut_ref_alti_station);

CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_date_fermeture_station
ON hubeau.hydrometry_stations(date_fermeture_station);

CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_date_maj_ref_alti_station
ON hubeau.hydrometry_stations(date_maj_ref_alti_station);

CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_date_maj_station
ON hubeau.hydrometry_stations(date_maj_station);

CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_date_ouverture_station
ON hubeau.hydrometry_stations(date_ouverture_station);

COMMENT ON TABLE hubeau.hydrometry_stations IS
'Table Hub''Eau: hydrometry_stations - PRIMARY KEY: code_station';
