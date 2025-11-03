-- Table: ecoulement_campagnes
-- Source: Hub'Eau API
-- PRIMARY KEY: code_campagne

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.ecoulement_campagnes (
    code_campagne TEXT NOT NULL,
    code_departement TEXT,
    code_reseau TEXT,
    code_type_campagne TEXT,
    date_campagne TIMESTAMP,
    libelle_departement TEXT,
    libelle_reseau TEXT,
    libelle_type_campagne TEXT,
    nombre_modalite_ecoulement BIGINT,
    uri_reseau TEXT,
    PRIMARY KEY (code_campagne)
);
-- Index temporels
CREATE INDEX IF NOT EXISTS idx_ecoulement_campagnes_date_campagne
ON hubeau.ecoulement_campagnes(date_campagne);

COMMENT ON TABLE hubeau.ecoulement_campagnes IS
'Table Hub''Eau: ecoulement_campagnes - PRIMARY KEY: code_campagne';
