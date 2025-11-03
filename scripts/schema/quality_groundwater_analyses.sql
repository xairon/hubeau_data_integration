-- Table: quality_groundwater_analyses
-- Source: Hub'Eau API
-- PRIMARY KEY: code_bss, date_debut_prelevement, code_param

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.quality_groundwater_analyses (
    altitude DOUBLE PRECISION,
    bss_id TEXT,
    code_bassin_dce TEXT,
    code_bss TEXT NOT NULL,
    code_circonscription_administrative_bassin TEXT,
    code_fraction TEXT,
    code_insee_actuel TEXT,
    code_lieu_analyse TEXT,
    code_methode TEXT,
    code_param BIGINT NOT NULL,
    code_producteur TEXT,
    code_qualification TEXT,
    code_region TEXT,
    code_remarque_analyse TEXT,
    code_statut_analyse TEXT,
    code_type_point_eau TEXT,
    code_type_qualito TEXT,
    code_unite TEXT,
    codes_entite_hg_bdlisa TEXT,
    codes_groupe_parametre TEXT,
    codes_masse_eau_edl TEXT,
    codes_masse_eau_rap TEXT,
    codes_reseau TEXT,
    date_debut_prelevement TIMESTAMP NOT NULL,
    incertitude_analytique DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    limite_detection DOUBLE PRECISION,
    limite_quantification DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    nom_bassin_dce TEXT,
    nom_circonscription_administrative_bassin TEXT,
    nom_commune_actuel TEXT,
    nom_departement TEXT,
    nom_fraction TEXT,
    nom_lieu_analyse TEXT,
    nom_methode TEXT,
    nom_param TEXT,
    nom_producteur TEXT,
    nom_qualification TEXT,
    nom_region TEXT,
    nom_remarque_analyse TEXT,
    nom_statut_analyse TEXT,
    nom_type_point_eau TEXT,
    nom_type_qualito TEXT,
    nom_unite TEXT,
    noms_entite_hg_bdlisa TEXT,
    noms_groupe_parametre TEXT,
    noms_masse_eau_edl TEXT,
    noms_masse_eau_rap TEXT,
    noms_reseau TEXT,
    num_departement TEXT,
    precision_coordonnees DOUBLE PRECISION,
    resultat DOUBLE PRECISION,
    seuil_saturation DOUBLE PRECISION,
    symbole_unite TEXT,
    uri_fraction TEXT,
    uri_lieu_analyse TEXT,
    uri_methode TEXT,
    uri_param TEXT,
    uri_producteur TEXT,
    uri_qualification TEXT,
    uri_remarque_analyse TEXT,
    uri_statut_analyse TEXT,
    uri_type_qualito TEXT,
    uri_unite TEXT,
    uris_groupe_parametre TEXT,
    uris_reseau TEXT,
    urn_bassin_dce TEXT,
    urn_bss TEXT,
    urns_bdlisa TEXT,
    urns_masse_eau_edl TEXT,
    urns_masse_eau_rap TEXT,
    PRIMARY KEY (code_bss, date_debut_prelevement, code_param)
);

-- Foreign Key: code_bss -> quality_groundwater_stations.code_bss
ALTER TABLE hubeau.quality_groundwater_analyses DROP CONSTRAINT IF EXISTS fk_quality_groundwater_analyses_code_bss;
ALTER TABLE hubeau.quality_groundwater_analyses
ADD CONSTRAINT fk_quality_groundwater_analyses_code_bss
FOREIGN KEY (code_bss) REFERENCES hubeau.quality_groundwater_stations(code_bss)
ON DELETE CASCADE;

-- Index temporels
CREATE INDEX IF NOT EXISTS idx_quality_groundwater_analyses_date_debut_prelevement
ON hubeau.quality_groundwater_analyses(date_debut_prelevement);

COMMENT ON TABLE hubeau.quality_groundwater_analyses IS
'Table Hub''Eau: quality_groundwater_analyses - PRIMARY KEY: code_bss, date_debut_prelevement, code_param';
