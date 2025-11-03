-- Table: prelevements_ouvrages
-- Source: Hub'Eau API
-- PRIMARY KEY: code_ouvrage

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.prelevements_ouvrages (
    code_bdlisa TEXT,
    code_commune_insee TEXT,
    code_departement TEXT,
    code_entite_hydro_cours_eau TEXT,
    code_entite_hydro_plan_eau TEXT,
    code_mer_ocean TEXT,
    code_ouvrage TEXT NOT NULL,
    code_point_referent TEXT,
    code_precision_coord TEXT,
    code_type_milieu TEXT,
    commentaire TEXT,
    date_exploitation_debut TIMESTAMP,
    date_exploitation_fin TEXT,
    id_local_ouvrage TEXT,
    latitude DOUBLE PRECISION,
    libelle_departement TEXT,
    libelle_precision_coord TEXT,
    libelle_type_milieu TEXT,
    longitude DOUBLE PRECISION,
    nom_commune TEXT,
    nom_ouvrage TEXT,
    ressource_cont_non_referencee BOOLEAN,
    ressource_cont_non_referencee_info TEXT,
    uri_bdlisa TEXT,
    uri_entite_hydro_cours_eau TEXT,
    uri_entite_hydro_plan_eau TEXT,
    uri_ouvrage TEXT,
    PRIMARY KEY (code_ouvrage)
);
-- Index temporels
CREATE INDEX IF NOT EXISTS idx_prelevements_ouvrages_date_exploitation_debut
ON hubeau.prelevements_ouvrages(date_exploitation_debut);

COMMENT ON TABLE hubeau.prelevements_ouvrages IS
'Table Hub''Eau: prelevements_ouvrages - PRIMARY KEY: code_ouvrage';
