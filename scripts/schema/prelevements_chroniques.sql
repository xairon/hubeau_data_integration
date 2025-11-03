-- Table: prelevements_chroniques
-- Source: Hub'Eau API
-- PRIMARY KEY: code_ouvrage, annee

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.prelevements_chroniques (
    annee BIGINT NOT NULL,
    code_commune_insee TEXT,
    code_departement TEXT,
    code_mode_obtention_volume TEXT,
    code_ouvrage TEXT NOT NULL,
    code_qualification_volume TEXT,
    code_statut_instruction TEXT,
    code_statut_volume TEXT,
    code_usage TEXT,
    latitude DOUBLE PRECISION,
    libelle_departement TEXT,
    libelle_mode_obtention_volume TEXT,
    libelle_qualification_volume TEXT,
    libelle_statut_instruction TEXT,
    libelle_statut_volume TEXT,
    libelle_usage TEXT,
    longitude DOUBLE PRECISION,
    nom_commune TEXT,
    nom_ouvrage TEXT,
    prelevement_ecrasant BOOLEAN,
    producteur_donnee TEXT,
    volume DOUBLE PRECISION,
    PRIMARY KEY (code_ouvrage, annee)
);
COMMENT ON TABLE hubeau.prelevements_chroniques IS
'Table Hub''Eau: prelevements_chroniques - PRIMARY KEY: code_ouvrage, annee';
