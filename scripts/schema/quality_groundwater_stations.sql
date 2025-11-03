-- Table: quality_groundwater_stations
-- Source: Hub'Eau API
-- PRIMARY KEY: code_bss

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.quality_groundwater_stations (
    altitude DOUBLE PRECISION,
    bassin_dce TEXT,
    bss_id TEXT,
    circonscriptions_administrative_bassin TEXT,
    code_bss TEXT NOT NULL,
    code_caracteristique_aquifere TEXT,
    code_etat_pe TEXT,
    code_insee TEXT,
    code_mode_gisement TEXT,
    code_nature_pe TEXT,
    codes_entite_hg_bdlisa TEXT,
    codes_masse_eau_edl TEXT,
    codes_masse_eau_rap TEXT,
    codes_reseau TEXT,
    commentaire_pe TEXT,
    date_debut_mesure TIMESTAMP,
    date_fin_mesure TIMESTAMP,
    latitude DOUBLE PRECISION,
    libelle_pe TEXT,
    longitude DOUBLE PRECISION,
    nom_caracteristique_aquifere TEXT,
    nom_commune TEXT,
    nom_departement TEXT,
    nom_etat_pe TEXT,
    nom_mode_gisement TEXT,
    nom_nature_pe TEXT,
    nom_region TEXT,
    noms_entite_hg_bdlisa TEXT,
    noms_masse_eau_edl TEXT,
    noms_masse_eau_rap TEXT,
    noms_reseau TEXT,
    num_departement TEXT,
    precision_coordonnees DOUBLE PRECISION,
    profondeur_investigation DOUBLE PRECISION,
    uri_caracteristique_aquifere TEXT,
    uri_etat_pe TEXT,
    uri_mode_gisement TEXT,
    uri_nature_pe TEXT,
    uris_reseau TEXT,
    urn_bassin_dce TEXT,
    urn_bss TEXT,
    urns_bdlisa TEXT,
    urns_masse_eau_edl TEXT,
    urns_masse_eau_rap TEXT,
    PRIMARY KEY (code_bss)
);
-- Index temporels
CREATE INDEX IF NOT EXISTS idx_quality_groundwater_stations_date_debut_mesure
ON hubeau.quality_groundwater_stations(date_debut_mesure);

CREATE INDEX IF NOT EXISTS idx_quality_groundwater_stations_date_fin_mesure
ON hubeau.quality_groundwater_stations(date_fin_mesure);

COMMENT ON TABLE hubeau.quality_groundwater_stations IS
'Table Hub''Eau: quality_groundwater_stations - PRIMARY KEY: code_bss';
