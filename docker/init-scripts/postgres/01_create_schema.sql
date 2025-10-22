-- Script d'initialisation de la base de données Hub'Eau
-- Exécuté automatiquement au démarrage du container PostgreSQL
-- Basé sur la documentation complète des APIs Hub'Eau

-- Créer le schéma hubeau
CREATE SCHEMA IF NOT EXISTS hubeau;

-- Extension PostGIS pour les données géospatiales
CREATE EXTENSION IF NOT EXISTS postgis;

-- Set search path
SET search_path TO hubeau, public;

-- ============================================
-- HYDROMÉTRIE (Débits et hauteurs)
-- ============================================

-- Table des sites hydrométriques (groupements de stations)
CREATE TABLE IF NOT EXISTS hubeau.hydrometry_sites (
    code_site VARCHAR(20) PRIMARY KEY,
    libelle_site VARCHAR(200),
    type_site VARCHAR(50),
    code_cours_eau VARCHAR(20),
    libelle_cours_eau VARCHAR(100),
    code_commune_site VARCHAR(10),
    libelle_commune VARCHAR(100),
    code_departement VARCHAR(3),
    libelle_departement VARCHAR(100),
    code_region VARCHAR(5),
    libelle_region VARCHAR(100),
    longitude_wgs84 DECIMAL(10, 6),
    latitude_wgs84 DECIMAL(10, 6),
    altitude DECIMAL(8, 2),
    surface_bv DECIMAL(12, 2),
    statut_site VARCHAR(50),
    date_ouverture DATE,
    date_fermeture DATE,
    producteur VARCHAR(100),
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des stations hydrométriques
-- Note: Pas de FK sur code_site car l'API Hub'Eau retourne des références incohérentes
--       (des stations référencent des sites qui n'existent pas dans /referentiel/sites)
CREATE TABLE IF NOT EXISTS hubeau.hydrometry_stations (
    code_station VARCHAR(20) PRIMARY KEY,
    code_site VARCHAR(20),  -- Pas de FK - données Hub'Eau incohérentes
    libelle_station VARCHAR(200),
    type_station VARCHAR(50),
    longitude_station DECIMAL(10, 6),
    latitude_station DECIMAL(10, 6),
    longitude_wgs84 DECIMAL(10, 6),
    latitude_wgs84 DECIMAL(10, 6),
    altitude_zero DECIMAL(8, 2),
    date_ouverture DATE,
    date_fermeture DATE,
    en_service BOOLEAN,
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des observations hydrométriques élaborées
CREATE TABLE IF NOT EXISTS hubeau.hydrometry_observations (
    id SERIAL PRIMARY KEY,
    code_station VARCHAR(20) REFERENCES hubeau.hydrometry_stations(code_station),
    date_obs TIMESTAMP NOT NULL,
    grandeur_hydro VARCHAR(10),
    resultat DECIMAL(12, 3),
    code_qualification VARCHAR(10),
    libelle_qualification VARCHAR(50),
    code_methode VARCHAR(20),
    libelle_methode VARCHAR(100),
    -- Partition key
    year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM date_obs)) STORED,
    -- Contrainte d'unicité
    UNIQUE(code_station, date_obs, grandeur_hydro),
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- PIÉZOMÉTRIE (Niveaux des nappes)
-- ============================================

-- Table des stations de piézométrie
CREATE TABLE IF NOT EXISTS hubeau.piezometry_stations (
    code_bss VARCHAR(20) PRIMARY KEY,
    urn_bss VARCHAR(100),
    date_recherche DATE,
    bss_id VARCHAR(20),
    code_commune_insee VARCHAR(10),
    libelle_commune VARCHAR(100),
    code_departement VARCHAR(3),
    libelle_departement VARCHAR(100),
    code_region VARCHAR(5),
    libelle_region VARCHAR(100),
    coordonnees_x DECIMAL(10, 6),
    coordonnees_y DECIMAL(10, 6),
    longitude_wgs84 DECIMAL(10, 6),
    latitude_wgs84 DECIMAL(10, 6),
    altitude_sol DECIMAL(8, 2),
    altitude_repere DECIMAL(8, 2),
    profondeur_totale DECIMAL(8, 2),
    code_entite_hydrogeo VARCHAR(20),
    libelle_entite_hydrogeo VARCHAR(200),
    code_masse_eau VARCHAR(20),
    libelle_masse_eau VARCHAR(200),
    nature_eau VARCHAR(50),
    milieu VARCHAR(50),
    niveau_acces_donnees VARCHAR(20),
    producteur_donnees VARCHAR(100),
    date_debut_mesure DATE,
    date_fin_mesure DATE,
    nb_mesures INTEGER,
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des chroniques piézométriques
CREATE TABLE IF NOT EXISTS hubeau.piezometry_chroniques (
    id SERIAL PRIMARY KEY,
    code_bss VARCHAR(20) REFERENCES hubeau.piezometry_stations(code_bss),
    date_mesure TIMESTAMP NOT NULL,
    niveau_eau_ngf DECIMAL(10, 3),
    niveau_eau_relative DECIMAL(10, 3),
    profondeur_nappe DECIMAL(10, 3),
    niveau_eau_indicateur DECIMAL(10, 3),
    qualification VARCHAR(20),
    statut VARCHAR(20),
    mode_obtention VARCHAR(50),
    continuité VARCHAR(20),
    producteur VARCHAR(100),
    -- Partition key
    year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM date_mesure)) STORED,
    -- Contrainte d'unicité
    UNIQUE(code_bss, date_mesure),
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- QUALITÉ DES EAUX - COURS D'EAU
-- ============================================

-- Table des stations de qualité cours d'eau
CREATE TABLE IF NOT EXISTS hubeau.quality_rivers_stations (
    code_station VARCHAR(20) PRIMARY KEY,
    libelle_station VARCHAR(200),
    code_cours_eau VARCHAR(20),
    libelle_cours_eau VARCHAR(100),
    code_commune VARCHAR(10),
    libelle_commune VARCHAR(100),
    code_departement VARCHAR(3),
    libelle_departement VARCHAR(100),
    code_region VARCHAR(5),
    libelle_region VARCHAR(100),
    longitude_wgs84 DECIMAL(10, 6),
    latitude_wgs84 DECIMAL(10, 6),
    code_masse_eau VARCHAR(20),
    libelle_masse_eau VARCHAR(200),
    statut_station VARCHAR(50),
    date_ouverture DATE,
    date_fermeture DATE,
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des opérations de prélèvement qualité cours d'eau
CREATE TABLE IF NOT EXISTS hubeau.quality_rivers_operations (
    id SERIAL PRIMARY KEY,
    code_station VARCHAR(20) REFERENCES hubeau.quality_rivers_stations(code_station),
    date_prelevement DATE NOT NULL,
    code_operation VARCHAR(20),
    heure_prelevement TIME,
    code_support VARCHAR(10),
    libelle_support VARCHAR(50),
    code_methode VARCHAR(20),
    libelle_methode VARCHAR(100),
    -- Contrainte d'unicité
    UNIQUE(code_station, date_prelevement, code_operation),
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des analyses qualité cours d'eau
CREATE TABLE IF NOT EXISTS hubeau.quality_rivers_analyses (
    id SERIAL PRIMARY KEY,
    code_station VARCHAR(20) REFERENCES hubeau.quality_rivers_stations(code_station),
    date_prelevement DATE NOT NULL,
    code_parametre VARCHAR(10),
    libelle_parametre VARCHAR(200),
    resultat DECIMAL(15, 6),
    unite VARCHAR(20),
    code_remarque VARCHAR(5),
    limite_detection DECIMAL(15, 6),
    limite_quantification DECIMAL(15, 6),
    code_statut VARCHAR(5),
    code_qualification VARCHAR(5),
    producteur VARCHAR(100),
    -- Partition key
    year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM date_prelevement)) STORED,
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des conditions environnementales qualité cours d'eau
CREATE TABLE IF NOT EXISTS hubeau.quality_rivers_conditions (
    id SERIAL PRIMARY KEY,
    code_station VARCHAR(20) REFERENCES hubeau.quality_rivers_stations(code_station),
    date_prelevement DATE NOT NULL,
    code_parametre VARCHAR(10),
    libelle_parametre VARCHAR(200),
    resultat DECIMAL(15, 6),
    code_unite VARCHAR(10),
    symbole_unite VARCHAR(20),
    code_qualification VARCHAR(5),
    libelle_qualification VARCHAR(50),
    -- Contrainte d'unicité
    UNIQUE(code_station, date_prelevement, code_parametre),
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- QUALITÉ DES EAUX - NAPPES SOUTERRAINES
-- ============================================

-- Table des stations qualité eau souterraine
CREATE TABLE IF NOT EXISTS hubeau.quality_groundwater_stations (
    code_bss VARCHAR(20) PRIMARY KEY,
    libelle_point VARCHAR(200),
    code_commune_insee VARCHAR(10),
    libelle_commune VARCHAR(100),
    code_departement VARCHAR(3),
    libelle_departement VARCHAR(100),
    code_region VARCHAR(5),
    libelle_region VARCHAR(100),
    longitude_wgs84 DECIMAL(10, 6),
    latitude_wgs84 DECIMAL(10, 6),
    altitude DECIMAL(8, 2),
    profondeur DECIMAL(8, 2),
    code_masse_eau VARCHAR(20),
    libelle_masse_eau VARCHAR(200),
    nature_point VARCHAR(50),
    usage_point VARCHAR(100),
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des analyses qualité eau souterraine
CREATE TABLE IF NOT EXISTS hubeau.quality_groundwater_analyses (
    id SERIAL PRIMARY KEY,
    code_bss VARCHAR(20) REFERENCES hubeau.quality_groundwater_stations(code_bss),
    date_prelevement DATE NOT NULL,
    code_parametre VARCHAR(10),
    libelle_parametre VARCHAR(200),
    resultat DECIMAL(15, 6),
    unite VARCHAR(20),
    code_remarque VARCHAR(5),
    limite_detection DECIMAL(15, 6),
    limite_quantification DECIMAL(15, 6),
    code_statut VARCHAR(5),
    code_qualification VARCHAR(5),
    producteur VARCHAR(100),
    -- Partition key
    year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM date_prelevement)) STORED,
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- TEMPÉRATURE DES COURS D'EAU
-- ============================================

-- Table des stations de température
CREATE TABLE IF NOT EXISTS hubeau.temperature_stations (
    code_station VARCHAR(20) PRIMARY KEY,
    libelle_station VARCHAR(200),
    code_cours_eau VARCHAR(20),
    libelle_cours_eau VARCHAR(100),
    code_commune VARCHAR(10),
    libelle_commune VARCHAR(100),
    longitude_wgs84 DECIMAL(10, 6),
    latitude_wgs84 DECIMAL(10, 6),
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des chroniques de température
CREATE TABLE IF NOT EXISTS hubeau.temperature_chroniques (
    id SERIAL PRIMARY KEY,
    code_station VARCHAR(20) REFERENCES hubeau.temperature_stations(code_station),
    date_mesure TIMESTAMP NOT NULL,
    temperature DECIMAL(5, 2),
    statut VARCHAR(20),
    qualification VARCHAR(20),
    -- Partition key
    year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM date_mesure)) STORED,
    -- Contrainte d'unicité
    UNIQUE(code_station, date_mesure),
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- ÉCOULEMENT (ONDE)
-- ============================================

-- Table des stations d'observation des écoulements
CREATE TABLE IF NOT EXISTS hubeau.ecoulement_stations (
    code_station VARCHAR(20) PRIMARY KEY,
    libelle_station VARCHAR(200),
    code_cours_eau VARCHAR(20),
    libelle_cours_eau VARCHAR(100),
    code_commune VARCHAR(10),
    libelle_commune VARCHAR(100),
    longitude_wgs84 DECIMAL(10, 6),
    latitude_wgs84 DECIMAL(10, 6),
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des campagnes d'observation
CREATE TABLE IF NOT EXISTS hubeau.ecoulement_campagnes (
    id SERIAL PRIMARY KEY,
    code_campagne VARCHAR(50),
    libelle_campagne VARCHAR(200),
    date_debut DATE,
    date_fin DATE,
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des observations d'écoulement
CREATE TABLE IF NOT EXISTS hubeau.ecoulement_observations (
    id SERIAL PRIMARY KEY,
    code_station VARCHAR(20) REFERENCES hubeau.ecoulement_stations(code_station),
    code_campagne VARCHAR(50),
    date_obs DATE NOT NULL,
    libelle_observation VARCHAR(200),
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- HYDROBIOLOGIE
-- ============================================

-- Table des stations hydrobiologiques
CREATE TABLE IF NOT EXISTS hubeau.hydrobio_stations (
    code_station VARCHAR(20) PRIMARY KEY,
    libelle_station VARCHAR(200),
    code_cours_eau VARCHAR(20),
    libelle_cours_eau VARCHAR(100),
    code_commune VARCHAR(10),
    libelle_commune VARCHAR(100),
    longitude_wgs84 DECIMAL(10, 6),
    latitude_wgs84 DECIMAL(10, 6),
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des indices biologiques
CREATE TABLE IF NOT EXISTS hubeau.hydrobio_indices (
    id SERIAL PRIMARY KEY,
    code_station VARCHAR(20) REFERENCES hubeau.hydrobio_stations(code_station),
    date_prelevement DATE NOT NULL,
    code_indice VARCHAR(20),
    libelle_indice VARCHAR(100),
    valeur DECIMAL(5, 2),
    classe_qualite VARCHAR(20),
    -- Contrainte d'unicité
    UNIQUE(code_station, date_prelevement, code_indice),
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des taxons
CREATE TABLE IF NOT EXISTS hubeau.hydrobio_taxons (
    id SERIAL PRIMARY KEY,
    code_station VARCHAR(20) REFERENCES hubeau.hydrobio_stations(code_station),
    date_prelevement DATE NOT NULL,
    code_taxon VARCHAR(20),
    libelle_taxon VARCHAR(200),
    abondance INTEGER,
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- PRÉLÈVEMENTS D'EAU
-- ============================================

-- Table des ouvrages de prélèvement
CREATE TABLE IF NOT EXISTS hubeau.prelevements_ouvrages (
    code_ouvrage VARCHAR(50) PRIMARY KEY,
    libelle_ouvrage VARCHAR(200),
    type_ouvrage VARCHAR(50),
    code_commune_insee VARCHAR(10),
    libelle_commune VARCHAR(100),
    code_departement VARCHAR(3),
    libelle_departement VARCHAR(100),
    code_region VARCHAR(5),
    libelle_region VARCHAR(100),
    longitude_wgs84 DECIMAL(10, 6),
    latitude_wgs84 DECIMAL(10, 6),
    code_milieu VARCHAR(20),
    libelle_milieu VARCHAR(100),
    code_usage VARCHAR(20),
    libelle_usage VARCHAR(100),
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des points de prélèvement
CREATE TABLE IF NOT EXISTS hubeau.prelevements_points (
    code_point VARCHAR(50) PRIMARY KEY,
    code_ouvrage VARCHAR(50) REFERENCES hubeau.prelevements_ouvrages(code_ouvrage),
    libelle_point VARCHAR(200),
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des prélèvements
CREATE TABLE IF NOT EXISTS hubeau.prelevements_chroniques (
    id SERIAL PRIMARY KEY,
    code_ouvrage VARCHAR(50) REFERENCES hubeau.prelevements_ouvrages(code_ouvrage),
    annee INTEGER NOT NULL,
    volume_preleve DECIMAL(15, 3),
    code_usage VARCHAR(20),
    libelle_usage VARCHAR(100),
    -- Contrainte d'unicité
    UNIQUE(code_ouvrage, annee, code_usage),
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- INDEX POUR OPTIMISATION
-- ============================================

-- Index sur les dates pour les requêtes temporelles
CREATE INDEX IF NOT EXISTS idx_piezometry_chroniques_date ON hubeau.piezometry_chroniques(date_mesure);
CREATE INDEX IF NOT EXISTS idx_piezometry_chroniques_year ON hubeau.piezometry_chroniques(year);
CREATE INDEX IF NOT EXISTS idx_hydrometry_observations_date ON hubeau.hydrometry_observations(date_obs);
CREATE INDEX IF NOT EXISTS idx_hydrometry_observations_year ON hubeau.hydrometry_observations(year);
CREATE INDEX IF NOT EXISTS idx_quality_rivers_analyses_date ON hubeau.quality_rivers_analyses(date_prelevement);
CREATE INDEX IF NOT EXISTS idx_quality_groundwater_analyses_date ON hubeau.quality_groundwater_analyses(date_prelevement);
CREATE INDEX IF NOT EXISTS idx_temperature_chroniques_date ON hubeau.temperature_chroniques(date_mesure);
CREATE INDEX IF NOT EXISTS idx_temperature_chroniques_year ON hubeau.temperature_chroniques(year);

-- Index sur les codes géographiques
CREATE INDEX IF NOT EXISTS idx_piezometry_stations_commune ON hubeau.piezometry_stations(code_commune_insee);
CREATE INDEX IF NOT EXISTS idx_piezometry_stations_dept ON hubeau.piezometry_stations(code_departement);
CREATE INDEX IF NOT EXISTS idx_hydrometry_sites_dept ON hubeau.hydrometry_sites(code_departement);

-- Index pour JOINs (sans FK, l'index n'est pas créé automatiquement)
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_code_site ON hubeau.hydrometry_stations(code_site);

-- Index géospatiaux PostGIS pour requêtes spatiales rapides
CREATE INDEX IF NOT EXISTS idx_piezometry_stations_geom ON hubeau.piezometry_stations USING GIST(ST_MakePoint(longitude_wgs84, latitude_wgs84));
CREATE INDEX IF NOT EXISTS idx_hydrometry_sites_geom ON hubeau.hydrometry_sites USING GIST(ST_MakePoint(longitude_wgs84, latitude_wgs84));
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_geom ON hubeau.hydrometry_stations USING GIST(ST_MakePoint(longitude_wgs84, latitude_wgs84));
CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_geom ON hubeau.quality_rivers_stations USING GIST(ST_MakePoint(longitude_wgs84, latitude_wgs84));
CREATE INDEX IF NOT EXISTS idx_quality_groundwater_stations_geom ON hubeau.quality_groundwater_stations USING GIST(ST_MakePoint(longitude_wgs84, latitude_wgs84));
CREATE INDEX IF NOT EXISTS idx_ecoulement_stations_geom ON hubeau.ecoulement_stations USING GIST(ST_MakePoint(longitude_wgs84, latitude_wgs84));
CREATE INDEX IF NOT EXISTS idx_hydrobio_stations_geom ON hubeau.hydrobio_stations USING GIST(ST_MakePoint(longitude_wgs84, latitude_wgs84));
CREATE INDEX IF NOT EXISTS idx_freelevements_ouvrages_geom ON hubeau.prelevements_ouvrages USING GIST(ST_MakePoint(longitude_wgs84, latitude_wgs84));
CREATE INDEX IF NOT EXISTS idx_temperature_stations_geom ON hubeau.temperature_stations USING GIST(ST_MakePoint(longitude_wgs84, latitude_wgs84));

-- ============================================
-- TRIGGERS POUR UPDATE AUTOMATIQUE
-- ============================================

-- Fonction pour mettre à jour le timestamp
CREATE OR REPLACE FUNCTION hubeau.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers pour les tables avec updated_at
CREATE TRIGGER update_piezometry_stations_modtime
    BEFORE UPDATE ON hubeau.piezometry_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();

CREATE TRIGGER update_hydrometry_sites_modtime
    BEFORE UPDATE ON hubeau.hydrometry_sites
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();

CREATE TRIGGER update_hydrometry_stations_modtime
    BEFORE UPDATE ON hubeau.hydrometry_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();

CREATE TRIGGER update_quality_rivers_stations_modtime
    BEFORE UPDATE ON hubeau.quality_rivers_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();

CREATE TRIGGER update_quality_groundwater_stations_modtime
    BEFORE UPDATE ON hubeau.quality_groundwater_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();

CREATE TRIGGER update_ecoulement_stations_modtime
    BEFORE UPDATE ON hubeau.ecoulement_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();

CREATE TRIGGER update_hydrobio_stations_modtime
    BEFORE UPDATE ON hubeau.hydrobio_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();

CREATE TRIGGER update_prelevements_ouvrages_modtime
    BEFORE UPDATE ON hubeau.prelevements_ouvrages
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();

CREATE TRIGGER update_temperature_stations_modtime
    BEFORE UPDATE ON hubeau.temperature_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();

-- ============================================
-- PERMISSIONS
-- ============================================

-- Grant usage on schema
GRANT USAGE ON SCHEMA hubeau TO PUBLIC;
GRANT CREATE ON SCHEMA hubeau TO PUBLIC;

-- Grant permissions on all tables
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA hubeau TO PUBLIC;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA hubeau TO PUBLIC;