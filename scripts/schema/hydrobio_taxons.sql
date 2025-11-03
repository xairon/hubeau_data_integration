-- Table: hydrobio_taxons
-- Source: Hub'Eau API
-- PRIMARY KEY: code_station_hydrobio, date_prelevement, code_support, code_appel_taxon

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.hydrobio_taxons (
    code_appel_taxon TEXT NOT NULL,
    code_banque_reference TEXT,
    code_bassin TEXT,
    code_commune TEXT,
    code_cours_eau TEXT,
    code_departement TEXT,
    code_lot TEXT,
    code_masse_eau TEXT,
    code_methode TEXT,
    code_operation_prelevement TEXT,
    code_prelevement TEXT,
    code_projection TEXT,
    code_qualification TEXT,
    code_region TEXT,
    code_sous_bassin TEXT,
    code_station_hydrobio TEXT NOT NULL,
    code_support TEXT NOT NULL,
    code_type_resultat TEXT,
    codes_indices_operation TEXT,
    codes_taxons_parents TEXT,
    coordonnee_x DOUBLE PRECISION,
    coordonnee_y DOUBLE PRECISION,
    date_prelevement TIMESTAMP NOT NULL,
    hauteur_moyenne_lame_eau DOUBLE PRECISION,
    largeur_moyenne_lame_eau DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    libelle_appel_taxon TEXT,
    libelle_bassin TEXT,
    libelle_commune TEXT,
    libelle_cours_eau TEXT,
    libelle_departement TEXT,
    libelle_liste_faune_flore TEXT,
    libelle_masse_eau TEXT,
    libelle_methode TEXT,
    libelle_qualification TEXT,
    libelle_region TEXT,
    libelle_sous_bassin TEXT,
    libelle_station_hydrobio TEXT,
    libelle_support TEXT,
    libelle_type_resultat TEXT,
    libelles_taxons_parents TEXT,
    longitude DOUBLE PRECISION,
    longueur_prospectee DOUBLE PRECISION,
    resultat_taxon DOUBLE PRECISION,
    uri_cours_eau TEXT,
    uri_masse_eau TEXT,
    uri_station_hydrobio TEXT,
    PRIMARY KEY (code_station_hydrobio, date_prelevement, code_support, code_appel_taxon)
);
-- Index temporels
CREATE INDEX IF NOT EXISTS idx_hydrobio_taxons_date_prelevement
ON hubeau.hydrobio_taxons(date_prelevement);

COMMENT ON TABLE hubeau.hydrobio_taxons IS
'Table Hub''Eau: hydrobio_taxons - PRIMARY KEY: code_station_hydrobio, date_prelevement, code_support, code_appel_taxon';
