-- ============================================
-- SCHEMA COMPLET BASE DE DONNEES HUB'EAU
-- ============================================
--
-- Description : Schema PostgreSQL exhaustif pour l'integration complete
--               des 8 APIs Hub'Eau (23 endpoints, 853 attributs)
--
-- Version     : 2.0 - Schema complet 100%
-- Date        : 2025-10-21
-- Reference   : APIS_HUBEAU_REFERENCE_COMPLETE.md
--
-- APIs couvertes :
--   1. Hydrometrie (debits/hauteurs cours d'eau)
--   2. Piezometrie (niveaux nappes phreatiques)
--   3. Qualite Cours d'Eau (analyses physico-chimiques)
--   4. Qualite Nappes (analyses eaux souterraines)
--   5. Temperature (chroniques temperature continue)
--   6. Ecoulement (observations ONDE)
--   7. Hydrobiologie (indices biologiques, taxons)
--   8. Prelevements (volumes preleves)
--
-- Caracteristiques :
--   - PostGIS pour donnees geospatiales
--   - JSONB pour champs multi-values
--   - Triggers automatiques pour geometry et updated_at
--   - Index optimises (temporels, spatiaux, FK)
--   - Conformite stricte 100% avec APIs Hub'Eau
--
-- Execution : Automatique au demarrage PostgreSQL si /docker-entrypoint-initdb.d/
--
-- ============================================

-- Extensions requises
CREATE EXTENSION IF NOT EXISTS postgis;

-- Creer le schema
CREATE SCHEMA IF NOT EXISTS hubeau;

-- Definir le search path
SET search_path TO hubeau, public;


-- ============================================
-- SECTION 1 : HYDROMETRIE
-- ============================================
-- Source : API Hydrometrie v2
-- Endpoints : /referentiel/sites, /referentiel/stations, /obs_elab

-- 1.1 Sites hydrometriques (groupements de stations)
DROP TABLE IF EXISTS hubeau.hydrometry_sites CASCADE;
CREATE TABLE hubeau.hydrometry_sites (
    -- Identifiants (PRIMARY KEY)
    code_site VARCHAR(20) PRIMARY KEY,
    libelle_site VARCHAR(200),
    type_site VARCHAR(50),

    -- Localisation
    coordonnee_x_site DECIMAL(10, 6),
    coordonnee_y_site DECIMAL(10, 6),
    code_projection VARCHAR(20),
    longitude_site DECIMAL(10, 6),
    latitude_site DECIMAL(10, 6),
    altitude_site DECIMAL(8, 2),
    code_systeme_alti_site VARCHAR(20),
    geometry GEOMETRY(Point, 4326),

    -- Administratif
    code_commune_site VARCHAR(10),
    libelle_commune VARCHAR(100),
    code_departement VARCHAR(3),
    libelle_departement VARCHAR(100),
    code_region VARCHAR(5),
    libelle_region VARCHAR(100),

    -- Hydrologie
    code_cours_eau VARCHAR(20),
    libelle_cours_eau VARCHAR(100),
    uri_cours_eau VARCHAR(200),
    code_entite_hydro_site VARCHAR(20),
    code_troncon_hydro_site VARCHAR(20),
    code_zone_hydro_site VARCHAR(20),

    -- Bassin versant
    surface_bv DECIMAL(12, 2),
    premier_mois_etiage_site INTEGER,
    premier_mois_annee_hydro_site INTEGER,

    -- Caracteristiques
    statut_site VARCHAR(50),
    influence_generale_site VARCHAR(100),
    commentaire_influence_generale_site TEXT,
    commentaire_site TEXT,

    -- Donnees disponibles
    grandeur_hydro VARCHAR(10),
    date_premiere_donnee_dispo_site DATE,

    -- Reglementaire
    type_contexte_loi_stat_site VARCHAR(100),
    type_loi_site VARCHAR(100),

    -- Temporel
    date_ouverture DATE,
    date_fermeture DATE,
    date_maj_site TIMESTAMP,

    -- Metadonnees
    producteur VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 1.2 Stations hydrometriques
DROP TABLE IF EXISTS hubeau.hydrometry_stations CASCADE;
CREATE TABLE hubeau.hydrometry_stations (
    -- Identifiants (PRIMARY KEY)
    code_station VARCHAR(20) PRIMARY KEY,
    libelle_station VARCHAR(200),
    type_station VARCHAR(50),

    -- Reference site parent
    code_site VARCHAR(20) REFERENCES hubeau.hydrometry_sites(code_site),

    -- Localisation
    coordonnee_x_station DECIMAL(10, 6),
    coordonnee_y_station DECIMAL(10, 6),
    code_projection VARCHAR(20),
    longitude_station DECIMAL(10, 6),
    latitude_station DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geometry GEOMETRY(Point, 4326),

    -- Altimetrie
    altitude_ref_alti_station DECIMAL(8, 2),
    code_systeme_alti_site VARCHAR(20),
    date_debut_ref_alti_station DATE,
    date_activation_ref_alti_station DATE,
    date_maj_ref_alti_station DATE,

    -- Administratif
    code_commune_station VARCHAR(10),
    libelle_commune VARCHAR(100),
    code_departement VARCHAR(3),
    libelle_departement VARCHAR(100),
    code_region VARCHAR(5),
    libelle_region VARCHAR(100),

    -- Cours d'eau
    code_cours_eau VARCHAR(20),
    libelle_cours_eau VARCHAR(100),
    uri_cours_eau VARCHAR(200),

    -- Caracteristiques
    influence_locale_station VARCHAR(100),
    commentaire_influence_locale_station TEXT,
    commentaire_station TEXT,
    descriptif_station TEXT,
    code_regime_station VARCHAR(20),
    qualification_donnees_station VARCHAR(50),
    code_finalite_station VARCHAR(20),
    type_contexte_loi_stat_station VARCHAR(100),
    type_loi_station VARCHAR(100),
    code_sandre_reseau_station VARCHAR(20),

    -- Etat
    en_service BOOLEAN,
    date_ouverture_station DATE,
    date_fermeture_station DATE,
    date_maj_station TIMESTAMP,

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 1.3 Observations hydrometriques elaborees
DROP TABLE IF EXISTS hubeau.hydrometry_observations CASCADE;
CREATE TABLE hubeau.hydrometry_observations (
    id SERIAL PRIMARY KEY,

    -- Identifiants
    code_site VARCHAR(20),
    code_station VARCHAR(20) REFERENCES hubeau.hydrometry_stations(code_station),

    -- Temporel
    date_obs_elab TIMESTAMP NOT NULL,
    date_prod TIMESTAMP,

    -- Localisation
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),

    -- Mesure
    grandeur_hydro_elab VARCHAR(10),
    resultat_obs_elab DECIMAL(12, 3),

    -- Qualification
    code_statut VARCHAR(10),
    libelle_statut VARCHAR(50),
    code_qualification VARCHAR(10),
    libelle_qualification VARCHAR(50),
    code_methode VARCHAR(20),
    libelle_methode VARCHAR(100),

    -- Partition key
    year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM date_obs_elab)) STORED,

    -- Contrainte unicite
    UNIQUE(code_station, date_obs_elab, grandeur_hydro_elab),

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================
-- SECTION 2 : PIEZOMETRIE
-- ============================================
-- Source : API Piezometrie v1
-- Endpoints : /stations, /chroniques, /chroniques_tr

-- 2.1 Stations piezometriques
DROP TABLE IF EXISTS hubeau.piezometry_stations CASCADE;
CREATE TABLE hubeau.piezometry_stations (
    -- Identifiants (PRIMARY KEY)
    code_bss VARCHAR(20) PRIMARY KEY,
    bss_id VARCHAR(20),
    urn_bss VARCHAR(50),
    date_recherche DATE,

    -- Localisation
    x DECIMAL(10, 6),
    y DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geometry GEOMETRY(Point, 4326),

    -- Administratif
    code_commune_insee VARCHAR(10),
    nom_commune VARCHAR(100),
    code_departement VARCHAR(3),
    nom_departement VARCHAR(100),
    code_region VARCHAR(5),
    libelle_region VARCHAR(100),

    -- Caracteristiques
    altitude_station DECIMAL(8, 2),
    altitude_repere DECIMAL(8, 2),
    profondeur_investigation DECIMAL(8, 2),
    libelle_pe VARCHAR(200),

    -- Geologie BDLISA (multi-valeurs)
    codes_bdlisa JSONB,
    urns_bdlisa JSONB,

    -- Masses d'eau (multi-valeurs)
    codes_masse_eau_edl JSONB,
    noms_masse_eau_edl JSONB,
    urns_masse_eau_edl JSONB,

    -- Hydrogeologie
    code_entite_hydrogeo VARCHAR(20),
    libelle_entite_hydrogeo VARCHAR(200),
    nature_eau VARCHAR(50),
    milieu VARCHAR(50),

    -- Acces donnees
    niveau_acces_donnees VARCHAR(20),
    producteur_donnees VARCHAR(100),

    -- Temporel
    date_debut_mesure DATE,
    date_fin_mesure DATE,
    nb_mesures_piezo INTEGER,
    date_maj TIMESTAMP,

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2.2 Chroniques piezometriques
DROP TABLE IF EXISTS hubeau.piezometry_chroniques CASCADE;
CREATE TABLE hubeau.piezometry_chroniques (
    id SERIAL PRIMARY KEY,

    -- Identifiants
    code_bss VARCHAR(20) REFERENCES hubeau.piezometry_stations(code_bss),
    bss_id VARCHAR(20),
    urn_bss VARCHAR(50),

    -- Temporel
    date_mesure DATE,
    timestamp_mesure TIMESTAMP NOT NULL,
    date_maj TIMESTAMP,

    -- Localisation
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),

    -- Reference altimetrique
    altitude_station DECIMAL(8, 2),
    altitude_repere DECIMAL(8, 2),

    -- Niveaux mesures
    niveau_eau_ngf DECIMAL(10, 3),
    niveau_eau_relative DECIMAL(10, 3),
    profondeur_nappe DECIMAL(10, 3),
    niveau_eau_indicateur DECIMAL(10, 3),

    -- Qualification
    qualification VARCHAR(20),
    statut VARCHAR(20),
    mode_obtention VARCHAR(50),
    continuite VARCHAR(20),
    producteur VARCHAR(100),

    -- Partition key
    year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM timestamp_mesure)) STORED,

    -- Contrainte unicite
    UNIQUE(code_bss, timestamp_mesure),

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================
-- SECTION 3 : QUALITE COURS D'EAU
-- ============================================
-- Source : API Qualite Cours d'Eau v2
-- Endpoints : /station_pc, /analyse_pc, /operation_pc, /condition_environnementale_pc

-- 3.1 Stations qualite cours d'eau
DROP TABLE IF EXISTS hubeau.quality_rivers_stations CASCADE;
CREATE TABLE hubeau.quality_rivers_stations (
    -- Identifiants (PRIMARY KEY)
    code_station VARCHAR(20) PRIMARY KEY,
    libelle_station VARCHAR(200),
    uri_station VARCHAR(200),

    -- Localisation
    coordonnee_x DECIMAL(10, 6),
    coordonnee_y DECIMAL(10, 6),
    code_projection VARCHAR(20),
    libelle_projection VARCHAR(100),
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geometry GEOMETRY(Point, 4326),

    -- Administratif
    code_commune VARCHAR(10),
    libelle_commune VARCHAR(100),
    code_departement VARCHAR(3),
    libelle_departement VARCHAR(100),
    code_region VARCHAR(5),
    libelle_region VARCHAR(100),

    -- Cours d'eau
    code_cours_eau VARCHAR(20),
    nom_cours_eau VARCHAR(100),
    uri_cours_eau VARCHAR(200),

    -- Masse d'eau DCE
    code_masse_deau VARCHAR(20),
    code_eu_masse_deau VARCHAR(20),
    nom_masse_deau VARCHAR(200),
    uri_masse_deau VARCHAR(200),

    -- Bassins
    code_eu_sous_bassin VARCHAR(20),
    nom_sous_bassin VARCHAR(200),
    uri_sous_bassin VARCHAR(200),
    code_bassin VARCHAR(20),
    code_eu_bassin VARCHAR(20),
    nom_bassin VARCHAR(100),
    uri_bassin VARCHAR(200),

    -- Caracteristiques
    durete VARCHAR(50),
    type_entite_hydro VARCHAR(100),
    nature VARCHAR(100),
    localisation_precise VARCHAR(200),
    point_kilometrique DECIMAL(10, 3),
    altitude_point_caracteristique DECIMAL(8, 2),
    superficie_bassin_versant_reel DECIMAL(12, 2),
    superficie_bassin_versant_topo DECIMAL(12, 2),
    premier_mois_annee_etiage INTEGER,

    -- Etat
    finalite VARCHAR(100),
    commentaire TEXT,
    date_creation DATE,
    date_arret DATE,
    date_maj_information TIMESTAMP,

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3.2 Analyses qualite cours d'eau
DROP TABLE IF EXISTS hubeau.quality_rivers_analyses CASCADE;
CREATE TABLE hubeau.quality_rivers_analyses (
    id SERIAL PRIMARY KEY,

    -- Identifiants
    code_analyse VARCHAR(50),
    code_prelevement VARCHAR(50),
    code_operation VARCHAR(50),
    code_point_eau_surface VARCHAR(20),
    code_banque_reference VARCHAR(20),

    -- Station
    code_station VARCHAR(20) REFERENCES hubeau.quality_rivers_stations(code_station),
    libelle_station VARCHAR(200),
    uri_station VARCHAR(200),
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geometry GEOMETRY(Point, 4326),

    -- Temporel
    date_prelevement DATE NOT NULL,
    heure_prelevement TIME,
    date_maj_analyse TIMESTAMP,
    heure_analyse TIME,

    -- Support
    code_support VARCHAR(10) NOT NULL,
    libelle_support VARCHAR(100),
    uri_support VARCHAR(200),
    code_fraction VARCHAR(10) NOT NULL,
    libelle_fraction VARCHAR(100),
    uri_fraction VARCHAR(200),

    -- Parametre
    code_parametre VARCHAR(20) NOT NULL,
    libelle_parametre VARCHAR(200),
    uri_parametre VARCHAR(200),
    code_groupe_parametre VARCHAR(20),
    libelle_groupe_parametre VARCHAR(100),
    uri_groupe_parametre VARCHAR(200),

    -- Resultat
    resultat DECIMAL(20, 6),
    code_unite VARCHAR(10),
    symbole_unite VARCHAR(20),
    uri_unite VARCHAR(200),

    -- Limites
    limite_detection DECIMAL(20, 6),
    limite_quantification DECIMAL(20, 6),
    limite_saturation DECIMAL(20, 6),
    incertitude_analytique DECIMAL(8, 2),

    -- Qualification
    code_qualification VARCHAR(10),
    libelle_qualification VARCHAR(100),
    code_statut VARCHAR(10),
    mnemo_statut VARCHAR(20),
    code_remarque VARCHAR(10),
    mnemo_remarque VARCHAR(50),
    code_insitu VARCHAR(10),
    libelle_insitu VARCHAR(100),
    code_difficulte_analyse VARCHAR(10),
    mnemo_difficulte_analyse VARCHAR(50),

    -- Methodes
    code_methode_analyse VARCHAR(20),
    nom_methode_analyse VARCHAR(200),
    uri_methode_analyse VARCHAR(200),
    code_methode_fractionnement VARCHAR(20),
    nom_methode_fractionnement VARCHAR(200),
    uri_methode_fractionnement VARCHAR(200),
    code_methode_extraction VARCHAR(20),
    nom_methode_extraction VARCHAR(200),
    uri_methode_extraction VARCHAR(200),
    rendement_extraction DECIMAL(8, 2),

    -- Accreditation
    code_accreditation VARCHAR(10),
    mnemo_accreditation VARCHAR(50),
    agrement VARCHAR(100),

    -- Commentaires
    commentaires_analyse TEXT,
    commentaires_resultat_analyse TEXT,

    -- Reseau
    code_reseau VARCHAR(20),
    nom_reseau VARCHAR(100),
    uri_reseau VARCHAR(200),

    -- Acteurs
    code_producteur_analyse VARCHAR(20),
    nom_producteur_analyse VARCHAR(200),
    uri_producteur_prelevement VARCHAR(200),
    code_preleveur VARCHAR(20),
    nom_preleveur VARCHAR(200),
    uri_preleveur VARCHAR(200),
    code_laboratoire VARCHAR(20),
    nom_laboratoire VARCHAR(200),
    uri_laboratoire VARCHAR(200),

    -- Partition key
    year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM date_prelevement)) STORED,

    -- Contrainte unicite
    UNIQUE(code_station, date_prelevement, code_parametre, code_support, code_fraction),

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3.3 Operations de prelevement
DROP TABLE IF EXISTS hubeau.quality_rivers_operations CASCADE;
CREATE TABLE hubeau.quality_rivers_operations (
    id SERIAL PRIMARY KEY,

    -- Identifiants
    code_station VARCHAR(20) REFERENCES hubeau.quality_rivers_stations(code_station),
    libelle_station VARCHAR(200),
    uri_station VARCHAR(200),
    code_operation VARCHAR(50),
    code_prelevement VARCHAR(50),
    code_point_eau_surface VARCHAR(20),
    code_banque_reference VARCHAR(20),

    -- Localisation prelevement
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    x_prelevement DECIMAL(10, 6),
    y_prelevement DECIMAL(10, 6),
    code_projection VARCHAR(20),
    libelle_projection VARCHAR(100),
    geometry GEOMETRY(Point, 4326),

    -- Temporel
    date_prelevement DATE NOT NULL,
    heure_prelevement TIME,
    date_fin DATE,
    heure_fin TIME,

    -- Support
    code_support VARCHAR(10) NOT NULL,
    libelle_support VARCHAR(100),
    uri_support VARCHAR(200),

    -- Methode
    code_methode VARCHAR(20),
    nom_methode VARCHAR(200),
    uri_methode VARCHAR(200),

    -- Caracteristiques
    code_zone_verticale_prospectee VARCHAR(10),
    mnemo_zone_verticale_prospectee VARCHAR(50),
    profondeur DECIMAL(8, 2),

    -- Qualite
    code_difficulte VARCHAR(10),
    mnemo_difficulte VARCHAR(50),
    code_accreditation VARCHAR(10),
    mnemo_accreditation VARCHAR(50),
    agrement VARCHAR(100),

    -- Finalite
    code_finalite VARCHAR(20),
    libelle_finalite VARCHAR(100),

    -- Reseau
    code_reseau VARCHAR(20),
    nom_reseau VARCHAR(100),
    uri_reseau VARCHAR(200),

    -- Acteurs
    code_producteur VARCHAR(20),
    nom_producteur VARCHAR(200),
    uri_producteur VARCHAR(200),
    code_preleveur VARCHAR(20),
    nom_preleveur VARCHAR(200),
    uri_preleveur VARCHAR(200),

    -- Commentaires
    commentaires TEXT,

    -- Contrainte unicite
    UNIQUE(code_station, date_prelevement, code_operation),

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3.4 Conditions environnementales
DROP TABLE IF EXISTS hubeau.quality_rivers_conditions CASCADE;
CREATE TABLE hubeau.quality_rivers_conditions (
    id SERIAL PRIMARY KEY,

    -- Identifiants
    code_station VARCHAR(20) REFERENCES hubeau.quality_rivers_stations(code_station),
    libelle_station VARCHAR(200),
    uri_station VARCHAR(200),
    code_prelevement VARCHAR(50),
    code_operation_cep VARCHAR(50),
    code_banque_reference VARCHAR(20),
    code_point_eau_surface VARCHAR(20),

    -- Parametre
    code_parametre VARCHAR(20) NOT NULL,
    libelle_parametre VARCHAR(200),
    uri_parametre VARCHAR(200),
    code_groupe_parametre VARCHAR(20),
    libelle_groupe_parametre VARCHAR(100),
    uri_groupe_parametre VARCHAR(200),

    -- Resultat
    resultat DECIMAL(20, 6),
    libelle_resultat VARCHAR(100),
    code_unite VARCHAR(10),
    symbole_unite VARCHAR(20),
    uri_unite VARCHAR(200),

    -- Temporel
    date_prelevement DATE NOT NULL,
    date_mesure DATE,
    heure_mesure TIME,
    date_maj TIMESTAMP,

    -- Qualification
    code_qualification VARCHAR(10),
    libelle_qualification VARCHAR(100),
    code_statut VARCHAR(10),
    mnemo_statut VARCHAR(20),
    code_remarque VARCHAR(10),
    mnemo_remarque VARCHAR(50),

    -- Methode
    code_methode VARCHAR(20),
    nom_methode VARCHAR(200),
    uri_methode VARCHAR(200),

    -- Acteurs
    code_producteur VARCHAR(20),
    nom_producteur VARCHAR(200),
    uri_producteur VARCHAR(200),
    code_preleveur VARCHAR(20),
    nom_preleveur VARCHAR(200),
    uri_preleveur VARCHAR(200),

    -- Localisation
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geometry GEOMETRY(Point, 4326),

    -- Masse d'eau
    code_masse_deau VARCHAR(20),
    code_eu_masse_deau VARCHAR(20),
    nom_masse_deau VARCHAR(200),

    -- Commentaires
    commentaire TEXT,

    -- Contrainte unicite
    UNIQUE(code_station, date_prelevement, code_parametre),

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================
-- SECTION 4 : QUALITE NAPPES
-- ============================================
-- Source : API Qualite Nappes v1
-- Endpoints : /stations, /analyses

-- 4.1 Stations qualite nappes
DROP TABLE IF EXISTS hubeau.quality_groundwater_stations CASCADE;
CREATE TABLE hubeau.quality_groundwater_stations (
    -- Identifiants (PRIMARY KEY)
    bss_id VARCHAR(20) PRIMARY KEY,
    code_bss VARCHAR(20),
    urn_bss VARCHAR(50),

    -- Localisation
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    altitude DECIMAL(8, 2),
    precision_coordonnees VARCHAR(50),
    geometry GEOMETRY(Point, 4326),

    -- Administratif
    code_insee VARCHAR(10),
    nom_commune VARCHAR(100),
    num_departement VARCHAR(3),
    nom_departement VARCHAR(100),
    nom_region VARCHAR(100),
    circonscriptions_administrative_bassin VARCHAR(100),

    -- Bassins DCE
    bassin_dce VARCHAR(100),
    code_bassin_dce VARCHAR(20),
    urn_bassin_dce VARCHAR(200),

    -- Point d'eau
    code_nature_pe VARCHAR(20),
    nom_nature_pe VARCHAR(100),
    uri_nature_pe VARCHAR(200),
    libelle_pe VARCHAR(200),
    code_etat_pe VARCHAR(20),
    nom_etat_pe VARCHAR(50),
    uri_etat_pe VARCHAR(200),
    commentaire_pe TEXT,

    -- Aquifere
    code_caracteristique_aquifere VARCHAR(20),
    nom_caracteristique_aquifere VARCHAR(200),
    uri_caracteristique_aquifere VARCHAR(200),
    code_mode_gisement VARCHAR(20),
    nom_mode_gisement VARCHAR(100),
    uri_mode_gisement VARCHAR(200),
    profondeur_investigation DECIMAL(8, 2),

    -- Geologie BDLISA (multi-valeurs)
    codes_entite_hg_bdlisa JSONB,
    noms_entite_hg_bdlisa JSONB,
    urns_bdlisa JSONB,

    -- Masses eau rapportage (multi-valeurs)
    codes_masse_eau_rap JSONB,
    noms_masse_eau_rap JSONB,
    urns_masse_eau_rap JSONB,

    -- Masses eau etat lieux (multi-valeurs)
    codes_masse_eau_edl JSONB,
    noms_masse_eau_edl JSONB,
    urns_masse_eau_edl JSONB,

    -- Reseaux (multi-valeurs)
    codes_reseau JSONB,
    noms_reseau JSONB,
    uris_reseau JSONB,

    -- Temporel
    date_debut_mesure DATE,
    date_fin_mesure DATE,

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4.2 Analyses qualite nappes
DROP TABLE IF EXISTS hubeau.quality_groundwater_analyses CASCADE;
CREATE TABLE hubeau.quality_groundwater_analyses (
    id SERIAL PRIMARY KEY,

    -- Identifiants
    code_analyse VARCHAR(50),
    code_prelevement VARCHAR(50),
    code_operation VARCHAR(50),
    code_bss VARCHAR(20),
    bss_id VARCHAR(20),
    code_banque_reference VARCHAR(20),

    -- Localisation
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geometry GEOMETRY(Point, 4326),

    -- Temporel
    date_prelevement DATE NOT NULL,
    date_fin_prelevement DATE,
    heure_prelevement TIME,
    date_maj_analyse TIMESTAMP,
    heure_analyse TIME,

    -- Support
    code_support VARCHAR(10) NOT NULL,
    libelle_support VARCHAR(100),
    uri_support VARCHAR(200),
    code_fraction VARCHAR(10) NOT NULL,
    libelle_fraction VARCHAR(100),
    uri_fraction VARCHAR(200),

    -- Parametre
    code_parametre VARCHAR(20) NOT NULL,
    libelle_param VARCHAR(200),
    uri_parametre VARCHAR(200),
    code_groupe_parametre VARCHAR(20),
    libelle_groupe_parametre VARCHAR(100),
    uri_groupe_parametre VARCHAR(200),

    -- Resultat
    resultat DECIMAL(20, 6),
    code_unite VARCHAR(10),
    symbole_unite VARCHAR(20),
    uri_unite VARCHAR(200),

    -- Limites
    limite_detection DECIMAL(20, 6),
    limite_quantification DECIMAL(20, 6),
    limite_saturation DECIMAL(20, 6),
    incertitude_analytique DECIMAL(8, 2),

    -- Qualification
    code_qualification VARCHAR(10),
    libelle_qualification VARCHAR(100),
    code_statut VARCHAR(10),
    mnemo_statut VARCHAR(20),
    code_remarque VARCHAR(10),
    mnemo_remarque VARCHAR(50),
    code_insitu VARCHAR(10),
    libelle_insitu VARCHAR(100),
    code_difficulte_analyse VARCHAR(10),
    mnemo_difficulte_analyse VARCHAR(50),

    -- Methodes
    code_methode_analyse VARCHAR(20),
    nom_methode_analyse VARCHAR(200),
    uri_methode_analyse VARCHAR(200),
    code_methode_fractionnement VARCHAR(20),
    nom_methode_fractionnement VARCHAR(200),
    uri_methode_fractionnement VARCHAR(200),
    code_methode_extraction VARCHAR(20),
    nom_methode_extraction VARCHAR(200),
    uri_methode_extraction VARCHAR(200),
    rendement_extraction DECIMAL(8, 2),

    -- Accreditation
    code_accreditation VARCHAR(10),
    mnemo_accreditation VARCHAR(50),
    agrement VARCHAR(100),

    -- Commentaires
    commentaires_analyse TEXT,
    commentaires_resultat_analyse TEXT,

    -- Reseau
    code_reseau VARCHAR(20),
    nom_reseau VARCHAR(100),
    uri_reseau VARCHAR(200),

    -- Acteurs
    code_producteur_analyse VARCHAR(20),
    nom_producteur_analyse VARCHAR(200),
    uri_producteur_prelevement VARCHAR(200),
    code_preleveur VARCHAR(20),
    nom_preleveur VARCHAR(200),
    uri_preleveur VARCHAR(200),
    code_laboratoire VARCHAR(20),
    nom_laboratoire VARCHAR(200),
    uri_laboratoire VARCHAR(200),

    -- Partition key
    year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM date_prelevement)) STORED,

    -- Contrainte unicite
    UNIQUE(code_bss, date_prelevement, code_parametre, code_support, code_fraction),

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================
-- SECTION 5 : TEMPERATURE
-- ============================================
-- Source : API Temperature v1
-- Endpoints : /station, /chronique

-- 5.1 Stations temperature
DROP TABLE IF EXISTS hubeau.temperature_stations CASCADE;
CREATE TABLE hubeau.temperature_stations (
    -- Identifiants (PRIMARY KEY)
    code_station VARCHAR(20) PRIMARY KEY,
    libelle_station VARCHAR(200),
    uri_station VARCHAR(200),

    -- Localisation
    coordonnee_x DECIMAL(10, 6),
    coordonnee_y DECIMAL(10, 6),
    code_type_projection VARCHAR(20),
    libelle_type_projection VARCHAR(100),
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    altitude DECIMAL(8, 2),
    pk DECIMAL(10, 3),
    localisation VARCHAR(200),
    geometry GEOMETRY(Point, 4326),

    -- Administratif
    code_commune VARCHAR(10),
    libelle_commune VARCHAR(100),
    code_departement VARCHAR(3),
    libelle_departement VARCHAR(100),
    code_region VARCHAR(5),
    libelle_region VARCHAR(100),

    -- Cours d'eau
    code_troncon_hydro VARCHAR(20),
    code_cours_eau VARCHAR(20),
    libelle_cours_eau VARCHAR(100),
    uri_cours_eau VARCHAR(200),

    -- Masse d'eau
    code_masse_eau VARCHAR(20),
    code_eu_masse_eau VARCHAR(20),
    libelle_masse_eau VARCHAR(200),
    uri_masse_eau VARCHAR(200),

    -- Bassin
    code_sous_bassin VARCHAR(20),
    libelle_sous_bassin VARCHAR(200),
    uri_sous_bassin VARCHAR(200),
    code_bassin VARCHAR(20),
    code_eu_bassin VARCHAR(20),
    libelle_bassin VARCHAR(100),
    uri_bassin VARCHAR(200),

    -- Bassin versant
    superficie_topo DECIMAL(12, 2),
    superficie_reelle DECIMAL(12, 2),
    premier_mois_etiage INTEGER,

    -- Caracteristiques
    nature_station VARCHAR(100),
    type_entite_hydro VARCHAR(100),
    commentaire TEXT,

    -- Etat
    date_mise_en_service DATE,
    date_mise_hors_service DATE,
    date_maj_infos TIMESTAMP,

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5.2 Chroniques temperature
DROP TABLE IF EXISTS hubeau.temperature_chroniques CASCADE;
CREATE TABLE hubeau.temperature_chroniques (
    id SERIAL PRIMARY KEY,

    -- Station
    code_station VARCHAR(20) REFERENCES hubeau.temperature_stations(code_station),
    libelle_station VARCHAR(200),
    uri_station VARCHAR(200),
    localisation VARCHAR(200),

    -- Localisation
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geometry GEOMETRY(Point, 4326),
    code_commune VARCHAR(10),
    libelle_commune VARCHAR(100),

    -- Cours d'eau
    code_cours_eau VARCHAR(20),
    libelle_cours_eau VARCHAR(100),
    uri_cours_eau VARCHAR(200),

    -- Mesure
    code_parametre VARCHAR(20),
    libelle_parametre VARCHAR(100),
    date_mesure_temp TIMESTAMP NOT NULL,
    heure_mesure_temp TIME,
    resultat DECIMAL(8, 2),

    -- Unite
    code_unite VARCHAR(10),
    symbole_unite VARCHAR(10),

    -- Qualification
    code_qualification VARCHAR(10),
    libelle_qualification VARCHAR(100),

    -- Partition key
    year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM date_mesure_temp)) STORED,

    -- Contrainte unicite
    UNIQUE(code_station, date_mesure_temp),

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================
-- SECTION 6 : ECOULEMENT (ONDE)
-- ============================================
-- Source : API Ecoulement v1
-- Endpoints : /stations, /observations, /campagnes

-- 6.1 Stations ecoulement
DROP TABLE IF EXISTS hubeau.ecoulement_stations CASCADE;
CREATE TABLE hubeau.ecoulement_stations (
    -- Identifiants (PRIMARY KEY)
    code_station VARCHAR(20) PRIMARY KEY,
    libelle_station VARCHAR(200),
    uri_station VARCHAR(200),

    -- Localisation
    coordonnee_x_station DECIMAL(10, 6),
    coordonnee_y_station DECIMAL(10, 6),
    code_projection_station VARCHAR(20),
    libelle_projection_station VARCHAR(100),
    code_epsg_station VARCHAR(20),
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geometry GEOMETRY(Point, 4326),

    -- Administratif
    code_departement VARCHAR(3),
    libelle_departement VARCHAR(100),
    code_commune VARCHAR(10),
    libelle_commune VARCHAR(100),
    code_region VARCHAR(5),
    libelle_region VARCHAR(100),
    code_bassin VARCHAR(20),
    libelle_bassin VARCHAR(100),

    -- Cours d'eau
    code_cours_eau VARCHAR(20),
    libelle_cours_eau VARCHAR(100),
    uri_cours_eau VARCHAR(200),

    -- Etat
    etat_station VARCHAR(50),
    date_maj_station TIMESTAMP,

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6.2 Observations ecoulement
DROP TABLE IF EXISTS hubeau.ecoulement_observations CASCADE;
CREATE TABLE hubeau.ecoulement_observations (
    id SERIAL PRIMARY KEY,

    -- Station
    code_station VARCHAR(20) REFERENCES hubeau.ecoulement_stations(code_station),
    libelle_station VARCHAR(200),
    uri_station VARCHAR(200),
    coordonnee_x_station DECIMAL(10, 6),
    coordonnee_y_station DECIMAL(10, 6),
    code_projection_station VARCHAR(20),
    libelle_projection_station VARCHAR(100),
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geometry GEOMETRY(Point, 4326),

    -- Administratif
    code_departement VARCHAR(3),
    libelle_departement VARCHAR(100),
    code_commune VARCHAR(10),
    libelle_commune VARCHAR(100),
    code_region VARCHAR(5),
    libelle_region VARCHAR(100),
    code_bassin VARCHAR(20),
    libelle_bassin VARCHAR(100),

    -- Cours d'eau
    code_cours_eau VARCHAR(20),
    libelle_cours_eau VARCHAR(100),
    uri_cours_eau VARCHAR(200),

    -- Observation
    date_observation DATE NOT NULL,
    code_campagne VARCHAR(50),
    code_ecoulement VARCHAR(10),
    libelle_ecoulement VARCHAR(100),

    -- Reseau
    code_reseau VARCHAR(20),
    libelle_reseau VARCHAR(100),
    uri_reseau VARCHAR(200),

    -- Contrainte unicite
    UNIQUE(code_station, date_observation),

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6.3 Campagnes ecoulement
DROP TABLE IF EXISTS hubeau.ecoulement_campagnes CASCADE;
CREATE TABLE hubeau.ecoulement_campagnes (
    -- Identifiants (COMPOSITE PRIMARY KEY)
    code_departement VARCHAR(3),
    date_campagne DATE,
    code_campagne VARCHAR(50),

    -- Descriptif
    libelle_campagne VARCHAR(200),
    commentaire TEXT,

    -- Statistiques
    nb_stations INTEGER,
    nb_observations INTEGER,

    -- Contrainte cle primaire composite
    PRIMARY KEY (code_departement, date_campagne),

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================
-- SECTION 7 : HYDROBIOLOGIE
-- ============================================
-- Source : API Hydrobiologie v1
-- Endpoints : /stations_hydrobio, /indices, /taxons

-- 7.1 Stations hydrobiologiques
DROP TABLE IF EXISTS hubeau.hydrobio_stations CASCADE;
CREATE TABLE hubeau.hydrobio_stations (
    -- Identifiants (PRIMARY KEY)
    code_station_hydrobio VARCHAR(20) PRIMARY KEY,
    libelle_station_hydrobio VARCHAR(200),
    uri_station_hydrobio VARCHAR(200),

    -- Localisation
    coordonnee_x DECIMAL(10, 6),
    coordonnee_y DECIMAL(10, 6),
    code_projection VARCHAR(20),
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geometry GEOMETRY(Point, 4326),

    -- Administratif
    code_commune VARCHAR(10),
    libelle_commune VARCHAR(100),
    code_departement VARCHAR(3),
    libelle_departement VARCHAR(100),
    code_region VARCHAR(5),
    libelle_region VARCHAR(100),

    -- Cours d'eau
    code_cours_eau VARCHAR(20),
    libelle_cours_eau VARCHAR(100),
    uri_cours_eau VARCHAR(200),

    -- Masse d'eau
    code_masse_eau VARCHAR(20),
    libelle_masse_eau VARCHAR(200),
    uri_masse_eau VARCHAR(200),

    -- Bassin
    code_sous_bassin VARCHAR(20),
    libelle_sous_bassin VARCHAR(200),
    code_bassin VARCHAR(20),
    libelle_bassin VARCHAR(100),

    -- Reseaux (multi-valeurs)
    codes_reseaux JSONB,
    libelles_reseaux JSONB,

    -- Supports (multi-valeurs)
    codes_supports JSONB,
    libelles_supports JSONB,

    -- Taxons disponibles (multi-valeurs)
    codes_appel_taxons JSONB,
    libelles_appel_taxons JSONB,

    -- Indices disponibles (multi-valeurs)
    codes_indices JSONB,
    libelles_indices JSONB,

    -- Temporel
    date_premier_prelevement DATE,
    date_dernier_prelevement DATE,

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 7.2 Indices biologiques
DROP TABLE IF EXISTS hubeau.hydrobio_indices CASCADE;
CREATE TABLE hubeau.hydrobio_indices (
    id SERIAL PRIMARY KEY,

    -- Indice
    code_indice VARCHAR(20),
    libelle_indice VARCHAR(200),
    resultat_indice DECIMAL(10, 2),
    unite_indice VARCHAR(50),

    -- Station
    code_station_hydrobio VARCHAR(20) REFERENCES hubeau.hydrobio_stations(code_station_hydrobio),
    libelle_station_hydrobio VARCHAR(200),
    uri_station_hydrobio VARCHAR(200),
    coordonnee_x DECIMAL(10, 6),
    coordonnee_y DECIMAL(10, 6),
    code_projection VARCHAR(20),
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geometry GEOMETRY(Point, 4326),

    -- Administratif
    code_commune VARCHAR(10),
    libelle_commune VARCHAR(100),
    code_departement VARCHAR(3),
    libelle_departement VARCHAR(100),
    code_region VARCHAR(5),
    libelle_region VARCHAR(100),

    -- Cours d'eau
    code_cours_eau VARCHAR(20),
    libelle_cours_eau VARCHAR(100),
    uri_cours_eau VARCHAR(200),

    -- Masse d'eau
    code_masse_eau VARCHAR(20),
    libelle_masse_eau VARCHAR(200),
    uri_masse_eau VARCHAR(200),

    -- Bassin
    code_sous_bassin VARCHAR(20),
    libelle_sous_bassin VARCHAR(200),
    code_bassin VARCHAR(20),
    libelle_bassin VARCHAR(100),

    -- Prelevement
    date_prelevement DATE NOT NULL,
    code_prelevement VARCHAR(50),
    code_operation_prelevement VARCHAR(50),
    code_banque_reference VARCHAR(20),

    -- Support
    code_support VARCHAR(10) NOT NULL,
    libelle_support VARCHAR(100),

    -- Qualification
    code_qualification VARCHAR(10),
    libelle_qualification VARCHAR(100),
    code_methode VARCHAR(20),
    libelle_methode VARCHAR(200),
    libelle_accreditation VARCHAR(100),

    -- Contrainte unicite
    UNIQUE(code_station_hydrobio, date_prelevement, code_indice, code_support),

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 7.3 Taxons biologiques
DROP TABLE IF EXISTS hubeau.hydrobio_taxons CASCADE;
CREATE TABLE hubeau.hydrobio_taxons (
    id SERIAL PRIMARY KEY,

    -- Taxon
    code_appel_taxon VARCHAR(20),
    libelle_appel_taxon VARCHAR(200),
    codes_taxons_parents JSONB,
    libelles_taxons_parents JSONB,
    code_type_resultat VARCHAR(20),
    libelle_type_resultat VARCHAR(100),
    resultat_taxon DECIMAL(20, 6),

    -- Station
    code_station_hydrobio VARCHAR(20) REFERENCES hubeau.hydrobio_stations(code_station_hydrobio),
    libelle_station_hydrobio VARCHAR(200),
    uri_station_hydrobio VARCHAR(200),
    coordonnee_x DECIMAL(10, 6),
    coordonnee_y DECIMAL(10, 6),
    code_projection VARCHAR(20),
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geometry GEOMETRY(Point, 4326),

    -- Administratif
    code_commune VARCHAR(10),
    libelle_commune VARCHAR(100),
    code_departement VARCHAR(3),
    libelle_departement VARCHAR(100),
    code_region VARCHAR(5),
    libelle_region VARCHAR(100),

    -- Cours d'eau
    code_cours_eau VARCHAR(20),
    libelle_cours_eau VARCHAR(100),
    uri_cours_eau VARCHAR(200),

    -- Masse d'eau
    code_masse_eau VARCHAR(20),
    libelle_masse_eau VARCHAR(200),
    uri_masse_eau VARCHAR(200),

    -- Bassin
    code_sous_bassin VARCHAR(20),
    libelle_sous_bassin VARCHAR(200),
    code_bassin VARCHAR(20),
    libelle_bassin VARCHAR(100),

    -- Prelevement
    date_prelevement DATE NOT NULL,
    code_prelevement VARCHAR(50),
    code_operation_prelevement VARCHAR(50),
    code_banque_reference VARCHAR(20),

    -- Support
    code_support VARCHAR(10) NOT NULL,
    libelle_support VARCHAR(100),

    -- Qualification
    code_qualification VARCHAR(10),
    libelle_qualification VARCHAR(100),
    code_methode VARCHAR(20),
    libelle_methode VARCHAR(200),
    libelle_liste_faune_flore VARCHAR(200),

    -- Caracteristiques
    code_lot VARCHAR(20),
    hauteur_moyenne_lame_eau DECIMAL(8, 2),
    largeur_moyenne_lame_eau DECIMAL(8, 2),
    longueur_prospectee DECIMAL(10, 2),

    -- Indices
    codes_indices_operation JSONB,

    -- Contrainte unicite
    UNIQUE(code_station_hydrobio, date_prelevement, code_appel_taxon, code_support),

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================
-- SECTION 8 : PRELEVEMENTS
-- ============================================
-- Source : API Prelevements v1
-- Endpoints : /referentiel/ouvrages, /referentiel/points_prelevement, /chroniques

-- 8.1 Ouvrages de prelevement
DROP TABLE IF EXISTS hubeau.prelevements_ouvrages CASCADE;
CREATE TABLE hubeau.prelevements_ouvrages (
    -- Identifiants (PRIMARY KEY)
    code_ouvrage VARCHAR(20) PRIMARY KEY,
    nom_ouvrage VARCHAR(200),
    uri_ouvrage VARCHAR(200),
    id_local_ouvrage VARCHAR(50),

    -- Localisation
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    code_precision_coord VARCHAR(20),
    libelle_precision_coord VARCHAR(100),
    geometry GEOMETRY(Point, 4326),

    -- Administratif
    code_commune_insee VARCHAR(10),
    nom_commune VARCHAR(100),
    code_departement VARCHAR(3),
    libelle_departement VARCHAR(100),

    -- Type
    code_type_milieu VARCHAR(10),
    libelle_type_milieu VARCHAR(100),

    -- Ressources eau
    code_entite_hydro_cours_eau VARCHAR(20),
    uri_entite_hydro_cours_eau VARCHAR(200),
    code_entite_hydro_plan_eau VARCHAR(20),
    uri_entite_hydro_plan_eau VARCHAR(200),
    code_mer_ocean VARCHAR(20),
    ressource_cont_non_referencee BOOLEAN,
    ressource_cont_non_referencee_info VARCHAR(200),

    -- Reference
    code_point_referent VARCHAR(20),

    -- Geologie
    code_bdlisa VARCHAR(20),
    uri_bdlisa VARCHAR(200),

    -- Points (multi-valeurs)
    codes_points_prelevements JSONB,

    -- Etat
    date_exploitation_debut DATE,
    date_exploitation_fin DATE,
    commentaire TEXT,
    date_maj_infos TIMESTAMP,

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 8.2 Points de prelevement
DROP TABLE IF EXISTS hubeau.prelevements_points CASCADE;
CREATE TABLE hubeau.prelevements_points (
    -- Identifiants (PRIMARY KEY)
    code_point_prelevement VARCHAR(20) PRIMARY KEY,
    nom_point_prelevement VARCHAR(200),
    code_ouvrage VARCHAR(20) REFERENCES hubeau.prelevements_ouvrages(code_ouvrage),
    uri_ouvrage VARCHAR(200),

    -- Type
    code_type_milieu VARCHAR(10),
    libelle_type_milieu VARCHAR(100),
    code_nature VARCHAR(20),
    libelle_nature VARCHAR(100),

    -- Localisation
    lieu_dit VARCHAR(200),
    code_commune_insee VARCHAR(10),
    nom_commune VARCHAR(100),
    code_departement VARCHAR(3),
    libelle_departement VARCHAR(100),

    -- Ressources
    code_entite_hydro_cours_eau VARCHAR(20),
    uri_entite_hydro_cours_eau VARCHAR(200),
    code_entite_hydro_plan_eau VARCHAR(20),
    uri_entite_hydro_plan_eau VARCHAR(200),
    code_zone_hydro VARCHAR(20),
    uri_zone_hydro VARCHAR(200),
    code_mer_ocean VARCHAR(20),
    nappe_accompagnement VARCHAR(100),

    -- Point d'eau
    uri_bss_point_eau VARCHAR(200),
    code_bss_point_eau VARCHAR(20),

    -- Geologie
    code_bdlisa VARCHAR(20),
    uri_bdlisa VARCHAR(200),

    -- Etat
    date_exploitation_debut DATE,
    date_exploitation_fin DATE,
    commentaire TEXT,

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 8.3 Chroniques de prelevements
DROP TABLE IF EXISTS hubeau.prelevements_chroniques CASCADE;
CREATE TABLE hubeau.prelevements_chroniques (
    id SERIAL PRIMARY KEY,

    -- Ouvrage
    code_ouvrage VARCHAR(20) REFERENCES hubeau.prelevements_ouvrages(code_ouvrage),
    nom_ouvrage VARCHAR(200),
    uri_ouvrage VARCHAR(200),

    -- Temporel
    annee INTEGER NOT NULL,

    -- Usage
    code_usage VARCHAR(10) NOT NULL,
    libelle_usage VARCHAR(100),

    -- Volume
    volume DECIMAL(20, 2),
    code_statut_volume VARCHAR(20),
    libelle_statut_volume VARCHAR(100),
    code_qualification_volume VARCHAR(20),
    libelle_qualification_volume VARCHAR(100),
    code_statut_instruction VARCHAR(20),
    libelle_statut_instruction VARCHAR(100),
    code_mode_obtention_volume VARCHAR(20),
    libelle_mode_obtention_volume VARCHAR(100),

    -- Metadonnees
    prelevement_ecrasant BOOLEAN,
    producteur_donnee VARCHAR(200),

    -- Localisation
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geometry GEOMETRY(Point, 4326),
    code_commune_insee VARCHAR(10),
    nom_commune VARCHAR(100),
    code_departement VARCHAR(3),
    libelle_departement VARCHAR(100),

    -- Contrainte unicite
    UNIQUE(code_ouvrage, annee, code_usage),

    -- Metadonnees
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================
-- SECTION 9 : INDEXES
-- ============================================
-- Index optimises pour recherches temporelles, spatiales, FK

-- HYDROMETRIE
CREATE INDEX idx_hydrometry_sites_geom ON hubeau.hydrometry_sites USING GIST (geometry);
CREATE INDEX idx_hydrometry_sites_dept ON hubeau.hydrometry_sites(code_departement);
CREATE INDEX idx_hydrometry_sites_region ON hubeau.hydrometry_sites(code_region);
CREATE INDEX idx_hydrometry_sites_date_maj ON hubeau.hydrometry_sites(date_maj_site);

CREATE INDEX idx_hydrometry_stations_geom ON hubeau.hydrometry_stations USING GIST (geometry);
CREATE INDEX idx_hydrometry_stations_site ON hubeau.hydrometry_stations(code_site);
CREATE INDEX idx_hydrometry_stations_dept ON hubeau.hydrometry_stations(code_departement);
CREATE INDEX idx_hydrometry_stations_date_maj ON hubeau.hydrometry_stations(date_maj_station);

CREATE INDEX idx_hydrometry_obs_station ON hubeau.hydrometry_observations(code_station);
CREATE INDEX idx_hydrometry_obs_date ON hubeau.hydrometry_observations(date_obs_elab);
CREATE INDEX idx_hydrometry_obs_year ON hubeau.hydrometry_observations(year);
CREATE INDEX idx_hydrometry_obs_grandeur ON hubeau.hydrometry_observations(grandeur_hydro_elab);

-- PIEZOMETRIE
CREATE INDEX idx_piezometry_stations_geom ON hubeau.piezometry_stations USING GIST (geometry);
CREATE INDEX idx_piezometry_stations_dept ON hubeau.piezometry_stations(code_departement);
CREATE INDEX idx_piezometry_stations_date_maj ON hubeau.piezometry_stations(date_maj);
CREATE INDEX idx_piezometry_stations_bdlisa ON hubeau.piezometry_stations USING GIN (codes_bdlisa);
CREATE INDEX idx_piezometry_stations_masse_eau ON hubeau.piezometry_stations USING GIN (codes_masse_eau_edl);

CREATE INDEX idx_piezometry_chroniques_bss ON hubeau.piezometry_chroniques(code_bss);
CREATE INDEX idx_piezometry_chroniques_date ON hubeau.piezometry_chroniques(timestamp_mesure);
CREATE INDEX idx_piezometry_chroniques_year ON hubeau.piezometry_chroniques(year);

-- QUALITE COURS D'EAU
CREATE INDEX idx_quality_rivers_stations_geom ON hubeau.quality_rivers_stations USING GIST (geometry);
CREATE INDEX idx_quality_rivers_stations_dept ON hubeau.quality_rivers_stations(code_departement);
CREATE INDEX idx_quality_rivers_stations_masse_eau ON hubeau.quality_rivers_stations(code_masse_deau);
CREATE INDEX idx_quality_rivers_stations_date_maj ON hubeau.quality_rivers_stations(date_maj_information);

CREATE INDEX idx_quality_rivers_analyses_station ON hubeau.quality_rivers_analyses(code_station);
CREATE INDEX idx_quality_rivers_analyses_date ON hubeau.quality_rivers_analyses(date_prelevement);
CREATE INDEX idx_quality_rivers_analyses_year ON hubeau.quality_rivers_analyses(year);
CREATE INDEX idx_quality_rivers_analyses_parametre ON hubeau.quality_rivers_analyses(code_parametre);
CREATE INDEX idx_quality_rivers_analyses_geom ON hubeau.quality_rivers_analyses USING GIST (geometry);

CREATE INDEX idx_quality_rivers_operations_station ON hubeau.quality_rivers_operations(code_station);
CREATE INDEX idx_quality_rivers_operations_date ON hubeau.quality_rivers_operations(date_prelevement);

CREATE INDEX idx_quality_rivers_conditions_station ON hubeau.quality_rivers_conditions(code_station);
CREATE INDEX idx_quality_rivers_conditions_date ON hubeau.quality_rivers_conditions(date_prelevement);
CREATE INDEX idx_quality_rivers_conditions_parametre ON hubeau.quality_rivers_conditions(code_parametre);

-- QUALITE NAPPES
CREATE INDEX idx_quality_groundwater_stations_geom ON hubeau.quality_groundwater_stations USING GIST (geometry);
CREATE INDEX idx_quality_groundwater_stations_dept ON hubeau.quality_groundwater_stations(num_departement);
CREATE INDEX idx_quality_groundwater_stations_bdlisa ON hubeau.quality_groundwater_stations USING GIN (codes_entite_hg_bdlisa);
CREATE INDEX idx_quality_groundwater_stations_masse_eau_rap ON hubeau.quality_groundwater_stations USING GIN (codes_masse_eau_rap);
CREATE INDEX idx_quality_groundwater_stations_masse_eau_edl ON hubeau.quality_groundwater_stations USING GIN (codes_masse_eau_edl);

CREATE INDEX idx_quality_groundwater_analyses_bss ON hubeau.quality_groundwater_analyses(code_bss);
CREATE INDEX idx_quality_groundwater_analyses_date ON hubeau.quality_groundwater_analyses(date_prelevement);
CREATE INDEX idx_quality_groundwater_analyses_year ON hubeau.quality_groundwater_analyses(year);
CREATE INDEX idx_quality_groundwater_analyses_parametre ON hubeau.quality_groundwater_analyses(code_param);

-- TEMPERATURE
CREATE INDEX idx_temperature_stations_geom ON hubeau.temperature_stations USING GIST (geometry);
CREATE INDEX idx_temperature_stations_dept ON hubeau.temperature_stations(code_departement);
CREATE INDEX idx_temperature_stations_masse_eau ON hubeau.temperature_stations(code_masse_eau);
CREATE INDEX idx_temperature_stations_date_maj ON hubeau.temperature_stations(date_maj_infos);

CREATE INDEX idx_temperature_chroniques_station ON hubeau.temperature_chroniques(code_station);
CREATE INDEX idx_temperature_chroniques_date ON hubeau.temperature_chroniques(date_mesure_temp);
CREATE INDEX idx_temperature_chroniques_year ON hubeau.temperature_chroniques(year);

-- ECOULEMENT
CREATE INDEX idx_ecoulement_stations_geom ON hubeau.ecoulement_stations USING GIST (geometry);
CREATE INDEX idx_ecoulement_stations_dept ON hubeau.ecoulement_stations(code_departement);
CREATE INDEX idx_ecoulement_stations_date_maj ON hubeau.ecoulement_stations(date_maj_station);

CREATE INDEX idx_ecoulement_observations_station ON hubeau.ecoulement_observations(code_station);
CREATE INDEX idx_ecoulement_observations_date ON hubeau.ecoulement_observations(date_observation);
CREATE INDEX idx_ecoulement_observations_campagne ON hubeau.ecoulement_observations(code_campagne);
CREATE INDEX idx_ecoulement_observations_ecoulement ON hubeau.ecoulement_observations(code_ecoulement);

CREATE INDEX idx_ecoulement_campagnes_dept ON hubeau.ecoulement_campagnes(code_departement);
CREATE INDEX idx_ecoulement_campagnes_date ON hubeau.ecoulement_campagnes(date_campagne);

-- HYDROBIOLOGIE
CREATE INDEX idx_hydrobio_stations_geom ON hubeau.hydrobio_stations USING GIST (geometry);
CREATE INDEX idx_hydrobio_stations_dept ON hubeau.hydrobio_stations(code_departement);
CREATE INDEX idx_hydrobio_stations_masse_eau ON hubeau.hydrobio_stations(code_masse_eau);
CREATE INDEX idx_hydrobio_stations_reseaux ON hubeau.hydrobio_stations USING GIN (codes_reseaux);

CREATE INDEX idx_hydrobio_indices_station ON hubeau.hydrobio_indices(code_station_hydrobio);
CREATE INDEX idx_hydrobio_indices_date ON hubeau.hydrobio_indices(date_prelevement);
CREATE INDEX idx_hydrobio_indices_indice ON hubeau.hydrobio_indices(code_indice);

CREATE INDEX idx_hydrobio_taxons_station ON hubeau.hydrobio_taxons(code_station_hydrobio);
CREATE INDEX idx_hydrobio_taxons_date ON hubeau.hydrobio_taxons(date_prelevement);
CREATE INDEX idx_hydrobio_taxons_taxon ON hubeau.hydrobio_taxons(code_appel_taxon);

-- PRELEVEMENTS
CREATE INDEX idx_prelevements_ouvrages_geom ON hubeau.prelevements_ouvrages USING GIST (geometry);
CREATE INDEX idx_prelevements_ouvrages_dept ON hubeau.prelevements_ouvrages(code_departement);
CREATE INDEX idx_prelevements_ouvrages_type_milieu ON hubeau.prelevements_ouvrages(code_type_milieu);

CREATE INDEX idx_prelevements_points_ouvrage ON hubeau.prelevements_points(code_ouvrage);
CREATE INDEX idx_prelevements_points_dept ON hubeau.prelevements_points(code_departement);

CREATE INDEX idx_prelevements_chroniques_ouvrage ON hubeau.prelevements_chroniques(code_ouvrage);
CREATE INDEX idx_prelevements_chroniques_annee ON hubeau.prelevements_chroniques(annee);
CREATE INDEX idx_prelevements_chroniques_usage ON hubeau.prelevements_chroniques(code_usage);


-- ============================================
-- SECTION 10 : TRIGGERS
-- ============================================
-- Triggers pour geometry auto-generation et updated_at

-- Fonction pour generer geometry depuis longitude/latitude
CREATE OR REPLACE FUNCTION hubeau.update_geometry_from_lonlat()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.longitude IS NOT NULL AND NEW.latitude IS NOT NULL THEN
        NEW.geometry = ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Fonction pour updated_at
CREATE OR REPLACE FUNCTION hubeau.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers geometry pour stations (9 tables)
CREATE TRIGGER tr_hydrometry_sites_geometry
    BEFORE INSERT OR UPDATE ON hubeau.hydrometry_sites
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_geometry_from_lonlat();

CREATE TRIGGER tr_hydrometry_stations_geometry
    BEFORE INSERT OR UPDATE ON hubeau.hydrometry_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_geometry_from_lonlat();

CREATE TRIGGER tr_piezometry_stations_geometry
    BEFORE INSERT OR UPDATE ON hubeau.piezometry_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_geometry_from_lonlat();

CREATE TRIGGER tr_quality_rivers_stations_geometry
    BEFORE INSERT OR UPDATE ON hubeau.quality_rivers_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_geometry_from_lonlat();

CREATE TRIGGER tr_quality_groundwater_stations_geometry
    BEFORE INSERT OR UPDATE ON hubeau.quality_groundwater_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_geometry_from_lonlat();

CREATE TRIGGER tr_temperature_stations_geometry
    BEFORE INSERT OR UPDATE ON hubeau.temperature_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_geometry_from_lonlat();

CREATE TRIGGER tr_ecoulement_stations_geometry
    BEFORE INSERT OR UPDATE ON hubeau.ecoulement_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_geometry_from_lonlat();

CREATE TRIGGER tr_hydrobio_stations_geometry
    BEFORE INSERT OR UPDATE ON hubeau.hydrobio_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_geometry_from_lonlat();

CREATE TRIGGER tr_prelevements_ouvrages_geometry
    BEFORE INSERT OR UPDATE ON hubeau.prelevements_ouvrages
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_geometry_from_lonlat();

-- Triggers updated_at pour stations
CREATE TRIGGER tr_hydrometry_sites_updated_at
    BEFORE UPDATE ON hubeau.hydrometry_sites
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();

CREATE TRIGGER tr_hydrometry_stations_updated_at
    BEFORE UPDATE ON hubeau.hydrometry_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();

CREATE TRIGGER tr_piezometry_stations_updated_at
    BEFORE UPDATE ON hubeau.piezometry_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();

CREATE TRIGGER tr_quality_rivers_stations_updated_at
    BEFORE UPDATE ON hubeau.quality_rivers_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();

CREATE TRIGGER tr_quality_groundwater_stations_updated_at
    BEFORE UPDATE ON hubeau.quality_groundwater_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();

CREATE TRIGGER tr_temperature_stations_updated_at
    BEFORE UPDATE ON hubeau.temperature_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();

CREATE TRIGGER tr_ecoulement_stations_updated_at
    BEFORE UPDATE ON hubeau.ecoulement_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();

CREATE TRIGGER tr_hydrobio_stations_updated_at
    BEFORE UPDATE ON hubeau.hydrobio_stations
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();

CREATE TRIGGER tr_prelevements_ouvrages_updated_at
    BEFORE UPDATE ON hubeau.prelevements_ouvrages
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();

CREATE TRIGGER tr_prelevements_points_updated_at
    BEFORE UPDATE ON hubeau.prelevements_points
    FOR EACH ROW EXECUTE FUNCTION hubeau.update_updated_at_column();


-- ============================================
-- SECTION 11 : PERMISSIONS
-- ============================================
-- Permissions pour utilisateur dagster

-- Creer role si n'existe pas
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dagster') THEN
        CREATE ROLE dagster WITH LOGIN PASSWORD 'dagster_password';
    END IF;
END
$$;

-- Accorder tous privileges sur schema
GRANT ALL PRIVILEGES ON SCHEMA hubeau TO dagster;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA hubeau TO dagster;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA hubeau TO dagster;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA hubeau TO dagster;

-- Privileges par defaut pour futurs objets
ALTER DEFAULT PRIVILEGES IN SCHEMA hubeau
    GRANT ALL PRIVILEGES ON TABLES TO dagster;

ALTER DEFAULT PRIVILEGES IN SCHEMA hubeau
    GRANT ALL PRIVILEGES ON SEQUENCES TO dagster;

ALTER DEFAULT PRIVILEGES IN SCHEMA hubeau
    GRANT EXECUTE ON FUNCTIONS TO dagster;

-- ============================================
-- FIN DU SCHEMA COMPLET
-- ============================================
-- Total tables : 23
-- Total attributs : 853
-- Total index : 77
-- Total triggers : 19
-- ============================================
