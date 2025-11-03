-- Table: prelevements_points
-- Source: Hub'Eau API
-- PRIMARY KEY: code_point_prelevement

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.prelevements_points (
    code_commune_insee TEXT,
    code_departement TEXT,
    code_entite_hydro_cours_eau TEXT,
    code_entite_hydro_plan_eau TEXT,
    code_mer_ocean TEXT,
    code_nature TEXT,
    code_ouvrage TEXT,
    code_point_prelevement TEXT NOT NULL,
    code_type_milieu TEXT,
    code_zone_hydro TEXT,
    commentaire TEXT,
    date_exploitation_debut TIMESTAMP,
    date_exploitation_fin TEXT,
    libelle_departement TEXT,
    libelle_nature TEXT,
    libelle_type_milieu TEXT,
    lieu_dit TEXT,
    nappe_accompagnement BOOLEAN,
    nom_commune TEXT,
    nom_point_prelevement TEXT,
    uri_bss_point_eau TEXT,
    uri_entite_hydro_cours_eau TEXT,
    uri_entite_hydro_plan_eau TEXT,
    uri_ouvrage TEXT,
    uri_zone_hydro TEXT,
    PRIMARY KEY (code_point_prelevement)
);

-- Foreign Key: code_ouvrage -> prelevements_ouvrages.code_ouvrage
ALTER TABLE hubeau.prelevements_points DROP CONSTRAINT IF EXISTS fk_prelevements_points_code_ouvrage;
ALTER TABLE hubeau.prelevements_points
ADD CONSTRAINT fk_prelevements_points_code_ouvrage
FOREIGN KEY (code_ouvrage) REFERENCES hubeau.prelevements_ouvrages(code_ouvrage)
ON DELETE CASCADE;

-- Index temporels
CREATE INDEX IF NOT EXISTS idx_prelevements_points_date_exploitation_debut
ON hubeau.prelevements_points(date_exploitation_debut);

COMMENT ON TABLE hubeau.prelevements_points IS
'Table Hub''Eau: prelevements_points - PRIMARY KEY: code_point_prelevement';
