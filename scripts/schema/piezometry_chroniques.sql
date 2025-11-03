-- Table: piezometry_chroniques
-- Source: Hub'Eau API
-- PRIMARY KEY: code_bss, date_mesure

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.piezometry_chroniques (
    code_bss TEXT NOT NULL,
    urn_bss TEXT,
    date_mesure TIMESTAMP NOT NULL,
    timestamp_mesure BIGINT,
    niveau_nappe_eau DOUBLE PRECISION,
    mode_obtention TEXT,
    statut TEXT,
    qualification TEXT,
    code_continuite TEXT,
    nom_continuite TEXT,
    code_producteur TEXT,
    nom_producteur TEXT,
    code_nature_mesure TEXT,
    nom_nature_mesure TEXT,
    profondeur_nappe DOUBLE PRECISION,
    PRIMARY KEY (code_bss, date_mesure)
);

-- Foreign Key: code_bss -> piezometry_stations.code_bss
ALTER TABLE hubeau.piezometry_chroniques DROP CONSTRAINT IF EXISTS fk_piezometry_chroniques_code_bss;
ALTER TABLE hubeau.piezometry_chroniques
ADD CONSTRAINT fk_piezometry_chroniques_code_bss
FOREIGN KEY (code_bss) REFERENCES hubeau.piezometry_stations(code_bss)
ON DELETE CASCADE;

-- Index temporels
CREATE INDEX IF NOT EXISTS idx_piezometry_chroniques_date_mesure
ON hubeau.piezometry_chroniques(date_mesure);

COMMENT ON TABLE hubeau.piezometry_chroniques IS
'Table Hub''Eau: piezometry_chroniques - PRIMARY KEY: code_bss, date_mesure';
