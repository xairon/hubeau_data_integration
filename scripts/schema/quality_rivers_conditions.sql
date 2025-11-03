-- Table: quality_rivers_conditions
-- Source: Hub'Eau API
-- PRIMARY KEY: code_operation_cep

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.quality_rivers_conditions (
    code_banque_reference TEXT,
    code_eu_masse_deau TEXT,
    code_groupe_parametre TEXT,
    code_masse_deau TEXT,
    code_methode TEXT,
    code_operation_cep TEXT NOT NULL,
    code_parametre BIGINT,
    code_point_eau_surface TEXT,
    code_prelevement TEXT,
    code_preleveur TEXT,
    code_producteur TEXT,
    code_qualification TEXT,
    code_remarque TEXT,
    code_station BIGINT,
    code_statut TEXT,
    code_unite TEXT,
    commentaire TEXT,
    date_maj TIMESTAMP,
    date_mesure TIMESTAMP,
    date_prelevement TIMESTAMP,
    heure_mesure TEXT,
    latitude DOUBLE PRECISION,
    libelle_groupe_parametre TEXT,
    libelle_parametre TEXT,
    libelle_qualification TEXT,
    libelle_resultat TEXT,
    libelle_station TEXT,
    longitude DOUBLE PRECISION,
    mnemo_remarque TEXT,
    mnemo_statut TEXT,
    nom_masse_deau TEXT,
    nom_methode TEXT,
    nom_preleveur TEXT,
    nom_producteur TEXT,
    resultat TEXT,
    symbole_unite TEXT,
    uri_groupe_parametre TEXT,
    uri_methode TEXT,
    uri_parametre TEXT,
    uri_preleveur TEXT,
    uri_producteur TEXT,
    uri_station TEXT,
    uri_unite TEXT,
    PRIMARY KEY (code_operation_cep)
);
-- Index temporels
CREATE INDEX IF NOT EXISTS idx_quality_rivers_conditions_date_maj
ON hubeau.quality_rivers_conditions(date_maj);

CREATE INDEX IF NOT EXISTS idx_quality_rivers_conditions_date_mesure
ON hubeau.quality_rivers_conditions(date_mesure);

CREATE INDEX IF NOT EXISTS idx_quality_rivers_conditions_date_prelevement
ON hubeau.quality_rivers_conditions(date_prelevement);

COMMENT ON TABLE hubeau.quality_rivers_conditions IS
'Table Hub''Eau: quality_rivers_conditions - PRIMARY KEY: code_operation_cep';
