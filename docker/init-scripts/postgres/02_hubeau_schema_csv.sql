-- ========================================================
-- SCHEMA HUBEAU - INGESTION CSV DIRECTE
-- Genere automatiquement depuis analyse CSV
-- Nombre de tables: 22
-- Configuration: 8GB RAM, SSD
-- ========================================================

-- Creation du schema
CREATE SCHEMA IF NOT EXISTS hubeau;


-- PIEZOMETRY_STATIONS
-- URL: https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/stations.csv
-- Total records: ~23,206
CREATE TABLE IF NOT EXISTS hubeau.piezometry_stations (
code_bss TEXT NOT NULL,
urn_bss TEXT NOT NULL,
date_debut_mesure TEXT,
date_fin_mesure TEXT,
code_commune_insee BIGINT NOT NULL,
nom_commune TEXT NOT NULL,
x DOUBLE PRECISION NOT NULL,
y DOUBLE PRECISION NOT NULL,
codes_bdlisa TEXT,
urns_bdlisa TEXT,
bss_id TEXT NOT NULL,
altitude_station DOUBLE PRECISION NOT NULL,
nb_mesures_piezo BIGINT NOT NULL,
code_departement BIGINT NOT NULL,
nom_departement TEXT NOT NULL,
libelle_pe TEXT,
profondeur_investigation DOUBLE PRECISION,
codes_masse_eau_edl TEXT,
noms_masse_eau_edl TEXT,
urns_masse_eau_edl TEXT,
date_maj TEXT NOT NULL,
    PRIMARY KEY (code_bss)
)
;


-- QUALITY_GROUNDWATER_STATIONS
-- URL: https://hubeau.eaufrance.fr/api/v1/qualite_nappes/stations.csv
-- Total records: ~81,842
CREATE TABLE IF NOT EXISTS hubeau.quality_groundwater_stations (
bss_id TEXT NOT NULL,
code_bss TEXT NOT NULL,
urn_bss TEXT NOT NULL,
date_debut_mesure TEXT,
date_fin_mesure TEXT,
precision_coordonnees DOUBLE PRECISION,
longitude DOUBLE PRECISION NOT NULL,
latitude DOUBLE PRECISION NOT NULL,
altitude DOUBLE PRECISION,
code_insee TEXT,
nom_commune TEXT,
num_departement TEXT,
nom_departement TEXT,
nom_region TEXT,
circonscriptions_administrative_bassin TEXT,
bassin_dce TEXT,
urn_bassin_dce TEXT,
code_nature_pe DOUBLE PRECISION,
nom_nature_pe TEXT,
uri_nature_pe TEXT,
libelle_pe TEXT,
code_caracteristique_aquifere DOUBLE PRECISION,
nom_caracteristique_aquifere TEXT,
uri_caracteristique_aquifere TEXT,
code_etat_pe DOUBLE PRECISION,
nom_etat_pe TEXT,
uri_etat_pe TEXT,
code_mode_gisement DOUBLE PRECISION,
nom_mode_gisement TEXT,
uri_mode_gisement TEXT,
profondeur_investigation DOUBLE PRECISION,
commentaire_pe DOUBLE PRECISION,
codes_entite_hg_bdlisa TEXT,
noms_entite_hg_bdlisa TEXT,
urns_bdlisa TEXT,
codes_masse_eau_rap TEXT,
noms_masse_eau_rap TEXT,
urns_masse_eau_rap TEXT,
codes_masse_eau_edl TEXT,
noms_masse_eau_edl TEXT,
urns_masse_eau_edl TEXT,
codes_reseau TEXT,
noms_reseau TEXT,
uris_reseau TEXT,
    PRIMARY KEY (code_bss)
)
;


-- QUALITY_GROUNDWATER_ANALYSES
-- URL: https://hubeau.eaufrance.fr/api/v1/qualite_nappes/analyses.csv
-- Total records: ~150,540,336
CREATE TABLE IF NOT EXISTS hubeau.quality_groundwater_analyses (
bss_id TEXT NOT NULL,
code_bss TEXT NOT NULL,
urn_bss TEXT NOT NULL,
precision_coordonnees DOUBLE PRECISION,
longitude DOUBLE PRECISION NOT NULL,
latitude DOUBLE PRECISION NOT NULL,
altitude DOUBLE PRECISION,
code_insee_actuel DOUBLE PRECISION,
nom_commune_actuel DOUBLE PRECISION,
num_departement DOUBLE PRECISION,
nom_departement DOUBLE PRECISION,
code_region DOUBLE PRECISION,
nom_region DOUBLE PRECISION,
code_circonscription_administrative_bassin DOUBLE PRECISION,
nom_circonscription_administrative_bassin DOUBLE PRECISION,
code_bassin_dce DOUBLE PRECISION,
nom_bassin_dce DOUBLE PRECISION,
urn_bassin_dce DOUBLE PRECISION,
code_type_point_eau BIGINT NOT NULL,
nom_type_point_eau TEXT NOT NULL,
codes_entite_hg_bdlisa DOUBLE PRECISION,
noms_entite_hg_bdlisa DOUBLE PRECISION,
urns_bdlisa DOUBLE PRECISION,
codes_masse_eau_rap DOUBLE PRECISION,
noms_masse_eau_rap DOUBLE PRECISION,
urns_masse_eau_rap DOUBLE PRECISION,
codes_masse_eau_edl DOUBLE PRECISION,
noms_masse_eau_edl DOUBLE PRECISION,
urns_masse_eau_edl DOUBLE PRECISION,
codes_reseau DOUBLE PRECISION,
noms_reseau DOUBLE PRECISION,
uris_reseau DOUBLE PRECISION,
code_type_qualito BIGINT NOT NULL,
nom_type_qualito TEXT NOT NULL,
uri_type_qualito TEXT NOT NULL,
code_producteur BIGINT NOT NULL,
nom_producteur TEXT NOT NULL,
uri_producteur TEXT NOT NULL,
date_debut_prelevement TEXT NOT NULL,
code_param BIGINT NOT NULL,
nom_param TEXT NOT NULL,
uri_param TEXT NOT NULL,
code_fraction BIGINT NOT NULL,
nom_fraction TEXT NOT NULL,
uri_fraction TEXT NOT NULL,
resultat DOUBLE PRECISION NOT NULL,
code_remarque_analyse BIGINT NOT NULL,
nom_remarque_analyse TEXT NOT NULL,
uri_remarque_analyse TEXT NOT NULL,
code_lieu_analyse BIGINT NOT NULL,
nom_lieu_analyse TEXT NOT NULL,
uri_lieu_analyse TEXT NOT NULL,
code_methode BIGINT NOT NULL,
nom_methode TEXT NOT NULL,
uri_methode TEXT NOT NULL,
code_unite BIGINT NOT NULL,
nom_unite TEXT NOT NULL,
symbole_unite TEXT NOT NULL,
uri_unite TEXT NOT NULL,
code_statut_analyse BIGINT NOT NULL,
nom_statut_analyse TEXT NOT NULL,
uri_statut_analyse TEXT NOT NULL,
code_qualification BIGINT NOT NULL,
nom_qualification TEXT NOT NULL,
uri_qualification TEXT NOT NULL,
limite_quantification DOUBLE PRECISION,
limite_detection DOUBLE PRECISION,
seuil_saturation DOUBLE PRECISION,
incertitude_analytique DOUBLE PRECISION,
codes_groupe_parametre TEXT NOT NULL,
noms_groupe_parametre TEXT NOT NULL,
uris_groupe_parametre TEXT NOT NULL,
    PRIMARY KEY (date_debut_prelevement, code_param)
)
WITH (
    fillfactor = 90,  -- Optimisation pour updates frequents (merge)
    autovacuum_vacuum_scale_factor = 0.05  -- Vacuum plus frequent
)
;


-- QUALITY_RIVERS_STATIONS
-- URL: https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/station_pc.csv
-- Total records: ~24,324
CREATE TABLE IF NOT EXISTS hubeau.quality_rivers_stations (
code_station TEXT NOT NULL,
libelle_station TEXT NOT NULL,
uri_station TEXT NOT NULL,
durete DOUBLE PRECISION,
coordonnee_x DOUBLE PRECISION NOT NULL,
coordonnee_y DOUBLE PRECISION NOT NULL,
code_projection BIGINT NOT NULL,
libelle_projection TEXT NOT NULL,
longitude DOUBLE PRECISION NOT NULL,
latitude DOUBLE PRECISION NOT NULL,
code_commune BIGINT NOT NULL,
libelle_commune TEXT NOT NULL,
code_departement BIGINT NOT NULL,
libelle_departement TEXT NOT NULL,
code_region BIGINT NOT NULL,
libelle_region TEXT NOT NULL,
code_cours_eau TEXT,
nom_cours_eau TEXT,
uri_cours_eau TEXT,
code_masse_deau DOUBLE PRECISION,
code_eu_masse_deau DOUBLE PRECISION,
nom_masse_deau DOUBLE PRECISION,
uri_masse_deau DOUBLE PRECISION,
code_eu_sous_bassin DOUBLE PRECISION,
nom_sous_bassin DOUBLE PRECISION,
code_bassin DOUBLE PRECISION,
code_eu_bassin DOUBLE PRECISION,
nom_bassin DOUBLE PRECISION,
uri_bassin DOUBLE PRECISION,
type_entite_hydro BIGINT NOT NULL,
commentaire DOUBLE PRECISION,
date_creation TEXT NOT NULL,
date_arret TEXT,
date_maj_information TEXT NOT NULL,
finalite DOUBLE PRECISION,
localisation_precise TEXT NOT NULL,
nature TEXT NOT NULL,
altitude_point_caracteristique DOUBLE PRECISION NOT NULL,
point_kilometrique DOUBLE PRECISION,
premier_mois_annee_etiage DOUBLE PRECISION,
superficie_bassin_versant_reel DOUBLE PRECISION,
superficie_bassin_versant_topo DOUBLE PRECISION,
uri_sous_bassin DOUBLE PRECISION,
    PRIMARY KEY (code_station)
)
;


-- QUALITY_RIVERS_ANALYSES
-- URL: https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/analyse_pc.csv
-- Total records: ~268,610,188
CREATE TABLE IF NOT EXISTS hubeau.quality_rivers_analyses (
code_station BIGINT NOT NULL,
libelle_station TEXT NOT NULL,
uri_station TEXT NOT NULL,
code_support BIGINT NOT NULL,
libelle_support TEXT NOT NULL,
uri_support TEXT NOT NULL,
code_fraction BIGINT NOT NULL,
libelle_fraction TEXT NOT NULL,
uri_fraction TEXT NOT NULL,
date_prelevement TEXT NOT NULL,
heure_prelevement TEXT NOT NULL,
date_maj_analyse TEXT NOT NULL,
heure_analyse DOUBLE PRECISION,
code_parametre BIGINT NOT NULL,
libelle_parametre TEXT NOT NULL,
uri_parametre TEXT NOT NULL,
resultat DOUBLE PRECISION NOT NULL,
code_groupe_parametre TEXT NOT NULL,
libelle_groupe_parametre TEXT NOT NULL,
uri_groupe_parametre TEXT NOT NULL,
code_unite BIGINT NOT NULL,
symbole_unite TEXT NOT NULL,
uri_unite TEXT NOT NULL,
code_remarque BIGINT NOT NULL,
mnemo_remarque TEXT NOT NULL,
code_insitu BIGINT NOT NULL,
libelle_insitu TEXT NOT NULL,
code_difficulte_analyse BIGINT NOT NULL,
mnemo_difficulte_analyse TEXT NOT NULL,
limite_detection DOUBLE PRECISION,
limite_quantification DOUBLE PRECISION,
limite_saturation DOUBLE PRECISION,
incertitude_analytique DOUBLE PRECISION,
code_methode_fractionnement DOUBLE PRECISION,
nom_methode_fractionnement DOUBLE PRECISION,
uri_methode_fractionnement DOUBLE PRECISION,
code_methode_analyse BIGINT NOT NULL,
nom_methode_analyse TEXT NOT NULL,
uri_methode_analyse TEXT NOT NULL,
rendement_extraction DOUBLE PRECISION,
code_methode_extraction DOUBLE PRECISION,
nom_methode_extraction DOUBLE PRECISION,
uri_methode_extraction DOUBLE PRECISION,
code_accreditation BIGINT NOT NULL,
mnemo_accreditation TEXT NOT NULL,
agrement DOUBLE PRECISION,
code_statut BIGINT NOT NULL,
mnemo_statut TEXT NOT NULL,
code_qualification BIGINT NOT NULL,
libelle_qualification TEXT NOT NULL,
commentaires_analyse DOUBLE PRECISION,
commentaires_resultat_analyse DOUBLE PRECISION,
code_reseau BIGINT NOT NULL,
nom_reseau TEXT NOT NULL,
uri_reseau TEXT NOT NULL,
code_producteur_analyse BIGINT NOT NULL,
nom_producteur_analyse TEXT NOT NULL,
uri_producteur_prelevement TEXT NOT NULL,
code_preleveur BIGINT NOT NULL,
nom_preleveur TEXT NOT NULL,
uri_preleveur TEXT NOT NULL,
code_laboratoire BIGINT NOT NULL,
nom_laboratoire TEXT NOT NULL,
uri_laboratoire TEXT NOT NULL,
latitude DOUBLE PRECISION NOT NULL,
longitude DOUBLE PRECISION NOT NULL,
code_operation BIGINT NOT NULL,
code_point_eau_surface BIGINT NOT NULL,
code_banque_reference TEXT NOT NULL,
code_prelevement TEXT NOT NULL,
code_analyse TEXT NOT NULL,
    PRIMARY KEY (code_analyse)
)
WITH (
    fillfactor = 90,  -- Optimisation pour updates frequents (merge)
    autovacuum_vacuum_scale_factor = 0.05  -- Vacuum plus frequent
)
;


-- QUALITY_RIVERS_CONDITIONS
-- URL: https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/condition_environnementale_pc.csv
-- Total records: ~14,424,946
CREATE TABLE IF NOT EXISTS hubeau.quality_rivers_conditions (
code_station TEXT NOT NULL,
libelle_station TEXT NOT NULL,
uri_station TEXT NOT NULL,
date_prelevement TEXT NOT NULL,
code_parametre BIGINT NOT NULL,
libelle_parametre TEXT NOT NULL,
uri_parametre TEXT NOT NULL,
resultat TEXT NOT NULL,
code_unite TEXT NOT NULL,
symbole_unite TEXT NOT NULL,
uri_unite TEXT NOT NULL,
code_remarque BIGINT NOT NULL,
code_statut BIGINT NOT NULL,
mnemo_statut TEXT NOT NULL,
code_qualification BIGINT NOT NULL,
libelle_qualification TEXT NOT NULL,
commentaire DOUBLE PRECISION,
date_mesure TEXT NOT NULL,
heure_mesure TEXT NOT NULL,
code_methode BIGINT NOT NULL,
nom_methode TEXT NOT NULL,
uri_methode TEXT NOT NULL,
code_producteur BIGINT NOT NULL,
nom_producteur TEXT NOT NULL,
uri_producteur TEXT NOT NULL,
code_preleveur BIGINT NOT NULL,
nom_preleveur TEXT NOT NULL,
uri_preleveur TEXT NOT NULL,
libelle_resultat TEXT NOT NULL,
mnemo_remarque TEXT NOT NULL,
code_groupe_parametre TEXT NOT NULL,
libelle_groupe_parametre TEXT NOT NULL,
code_masse_deau DOUBLE PRECISION,
code_banque_reference TEXT NOT NULL,
code_point_eau_surface BIGINT NOT NULL,
code_prelevement DOUBLE PRECISION,
date_maj TEXT NOT NULL,
uri_groupe_parametre TEXT NOT NULL,
code_eu_masse_deau DOUBLE PRECISION,
code_operation_cep TEXT NOT NULL,
nom_masse_deau DOUBLE PRECISION,
latitude DOUBLE PRECISION,
longitude DOUBLE PRECISION,
    PRIMARY KEY (date_prelevement, code_parametre)
)
;


-- QUALITY_RIVERS_OPERATIONS
-- URL: https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/operation_pc.csv
CREATE TABLE IF NOT EXISTS hubeau.quality_rivers_operations (
code_station TEXT NOT NULL,
libelle_station TEXT NOT NULL,
uri_station TEXT NOT NULL,
longitude DOUBLE PRECISION NOT NULL,
latitude DOUBLE PRECISION NOT NULL,
x_prelevement DOUBLE PRECISION,
y_prelevement DOUBLE PRECISION,
code_projection BIGINT NOT NULL,
libelle_projection TEXT NOT NULL,
code_support BIGINT NOT NULL,
libelle_support TEXT NOT NULL,
uri_support TEXT NOT NULL,
code_methode DOUBLE PRECISION,
nom_methode DOUBLE PRECISION,
uri_methode DOUBLE PRECISION,
date_prelevement TEXT NOT NULL,
date_fin DOUBLE PRECISION,
heure_fin DOUBLE PRECISION,
code_zone_verticale_prospectee BIGINT NOT NULL,
mnemo_zone_verticale_prospectee TEXT NOT NULL,
profondeur DOUBLE PRECISION NOT NULL,
code_difficulte BIGINT NOT NULL,
mnemo_difficulte TEXT NOT NULL,
code_accreditation BIGINT NOT NULL,
mnemo_accreditation TEXT NOT NULL,
agrement DOUBLE PRECISION,
code_finalite BIGINT NOT NULL,
libelle_finalite TEXT NOT NULL,
commentaires DOUBLE PRECISION,
code_reseau DOUBLE PRECISION,
nom_reseau DOUBLE PRECISION,
uri_reseau DOUBLE PRECISION,
code_producteur BIGINT NOT NULL,
nom_producteur TEXT NOT NULL,
uri_producteur TEXT NOT NULL,
code_preleveur BIGINT NOT NULL,
nom_preleveur TEXT NOT NULL,
uri_preleveur TEXT NOT NULL,
code_operation TEXT NOT NULL,
code_prelevement BIGINT NOT NULL,
code_point_eau_surface BIGINT NOT NULL,
code_banque_reference TEXT NOT NULL,
heure_prelevement TEXT NOT NULL,
    PRIMARY KEY (code_prelevement)
)
;


-- TEMPERATURE_STATIONS
-- URL: https://hubeau.eaufrance.fr/api/v1/temperature/station.csv
-- Total records: ~850
CREATE TABLE IF NOT EXISTS hubeau.temperature_stations (
code_station BIGINT NOT NULL,
libelle_station TEXT NOT NULL,
uri_station TEXT NOT NULL,
localisation TEXT NOT NULL,
coordonnee_x DOUBLE PRECISION NOT NULL,
coordonnee_y DOUBLE PRECISION NOT NULL,
code_type_projection BIGINT NOT NULL,
longitude DOUBLE PRECISION NOT NULL,
latitude DOUBLE PRECISION NOT NULL,
code_commune TEXT NOT NULL,
libelle_commune TEXT NOT NULL,
code_departement TEXT NOT NULL,
libelle_departement TEXT NOT NULL,
code_region BIGINT NOT NULL,
libelle_region TEXT NOT NULL,
code_troncon_hydro TEXT,
code_cours_eau TEXT,
libelle_cours_eau TEXT,
uri_cours_eau TEXT,
code_masse_eau TEXT,
libelle_masse_eau TEXT,
uri_masse_eau TEXT,
code_sous_bassin TEXT,
libelle_sous_bassin TEXT,
code_bassin TEXT,
libelle_bassin TEXT,
uri_bassin TEXT,
pk DOUBLE PRECISION,
altitude DOUBLE PRECISION NOT NULL,
date_maj_infos TEXT NOT NULL,
    PRIMARY KEY (code_station)
)
;


-- HYDROMETRY_SITES
-- URL: https://hubeau.eaufrance.fr/api/v2/hydrometrie/referentiel/sites.csv
-- Total records: ~9,073
CREATE TABLE IF NOT EXISTS hubeau.hydrometry_sites (
code_site BIGINT NOT NULL,
libelle_site TEXT NOT NULL,
type_site TEXT NOT NULL,
coordonnee_x_site DOUBLE PRECISION NOT NULL,
coordonnee_y_site DOUBLE PRECISION NOT NULL,
code_projection BIGINT NOT NULL,
longitude_site DOUBLE PRECISION NOT NULL,
latitude_site DOUBLE PRECISION NOT NULL,
altitude_site DOUBLE PRECISION,
code_systeme_alti_site DOUBLE PRECISION,
surface_bv DOUBLE PRECISION,
statut_site BIGINT NOT NULL,
premier_mois_etiage_site BIGINT NOT NULL,
premier_mois_annee_hydro_site BIGINT NOT NULL,
influence_generale_site BIGINT NOT NULL,
code_entite_hydro_site TEXT NOT NULL,
code_troncon_hydro_site BIGINT NOT NULL,
code_commune_site BIGINT NOT NULL,
code_zone_hydro_site BIGINT NOT NULL,
libelle_commune TEXT NOT NULL,
code_departement BIGINT NOT NULL,
code_region BIGINT NOT NULL,
libelle_region TEXT NOT NULL,
code_cours_eau TEXT NOT NULL,
libelle_cours_eau TEXT,
uri_cours_eau TEXT NOT NULL,
grandeur_hydro TEXT NOT NULL,
date_maj_site TEXT NOT NULL,
date_premiere_donnee_dispo_site DOUBLE PRECISION,
commentaire_influence_generale_site TEXT,
commentaire_site DOUBLE PRECISION,
type_contexte_loi_stat_site DOUBLE PRECISION,
type_loi_site DOUBLE PRECISION,
libelle_departement TEXT NOT NULL,
    PRIMARY KEY (code_site)
)
;


-- HYDROMETRY_STATIONS
-- URL: https://hubeau.eaufrance.fr/api/v2/hydrometrie/referentiel/stations.csv
-- Total records: ~6,187
CREATE TABLE IF NOT EXISTS hubeau.hydrometry_stations (
code_site BIGINT NOT NULL,
libelle_site TEXT NOT NULL,
code_station BIGINT NOT NULL,
libelle_station TEXT NOT NULL,
type_station TEXT NOT NULL,
coordonnee_x_station DOUBLE PRECISION NOT NULL,
coordonnee_y_station DOUBLE PRECISION NOT NULL,
code_projection BIGINT NOT NULL,
longitude_station DOUBLE PRECISION NOT NULL,
latitude_station DOUBLE PRECISION NOT NULL,
influence_locale_station DOUBLE PRECISION,
commentaire_station TEXT,
altitude_ref_alti_station DOUBLE PRECISION,
code_systeme_alti_site DOUBLE PRECISION,
code_commune_station BIGINT NOT NULL,
libelle_commune TEXT NOT NULL,
code_departement BIGINT NOT NULL,
code_region BIGINT NOT NULL,
libelle_region TEXT NOT NULL,
code_cours_eau TEXT NOT NULL,
libelle_cours_eau TEXT,
uri_cours_eau TEXT NOT NULL,
descriptif_station TEXT,
date_maj_station TEXT NOT NULL,
date_ouverture_station TEXT NOT NULL,
date_fermeture_station TEXT,
commentaire_influence_locale_station TEXT,
code_regime_station BIGINT NOT NULL,
qualification_donnees_station BIGINT NOT NULL,
code_finalite_station TEXT,
type_contexte_loi_stat_station DOUBLE PRECISION,
type_loi_station DOUBLE PRECISION,
code_sandre_reseau_station TEXT,
date_debut_ref_alti_station TEXT,
date_activation_ref_alti_station TEXT,
date_maj_ref_alti_station TEXT,
libelle_departement TEXT NOT NULL,
en_service BOOLEAN NOT NULL,
coordLatLon TEXT NOT NULL,
    PRIMARY KEY (code_station)
)
;


-- HYDROMETRY_OBS_ELAB
-- URL: https://hubeau.eaufrance.fr/api/v2/hydrometrie/obs_elab.csv
-- Total records: ~263,284,468
CREATE TABLE IF NOT EXISTS hubeau.hydrometry_obs_elab (
code_site BIGINT NOT NULL,
code_station BIGINT NOT NULL,
date_obs_elab TEXT NOT NULL,
resultat_obs_elab DOUBLE PRECISION NOT NULL,
date_prod TEXT NOT NULL,
code_statut BIGINT NOT NULL,
libelle_statut TEXT NOT NULL,
code_methode BIGINT NOT NULL,
libelle_methode TEXT NOT NULL,
code_qualification BIGINT NOT NULL,
libelle_qualification TEXT NOT NULL,
longitude DOUBLE PRECISION NOT NULL,
latitude DOUBLE PRECISION NOT NULL,
grandeur_hydro_elab TEXT NOT NULL,
    PRIMARY KEY (code_site, date_obs_elab)
)
;


-- HYDROBIO_STATIONS
-- URL: https://hubeau.eaufrance.fr/api/v1/hydrobio/stations_hydrobio.csv
-- Total records: ~20,657
CREATE TABLE IF NOT EXISTS hubeau.hydrobio_stations (
code_station_hydrobio TEXT NOT NULL,
libelle_station_hydrobio TEXT NOT NULL,
uri_station_hydrobio TEXT NOT NULL,
coordonnee_x DOUBLE PRECISION NOT NULL,
coordonnee_y DOUBLE PRECISION NOT NULL,
code_projection BIGINT NOT NULL,
code_cours_eau TEXT,
libelle_cours_eau TEXT,
uri_cours_eau TEXT,
code_masse_eau DOUBLE PRECISION,
libelle_masse_eau DOUBLE PRECISION,
uri_masse_eau DOUBLE PRECISION,
code_sous_bassin DOUBLE PRECISION,
libelle_sous_bassin DOUBLE PRECISION,
code_bassin DOUBLE PRECISION,
libelle_bassin DOUBLE PRECISION,
code_commune BIGINT NOT NULL,
libelle_commune TEXT NOT NULL,
code_departement BIGINT NOT NULL,
libelle_departement TEXT NOT NULL,
code_region BIGINT NOT NULL,
libelle_region TEXT NOT NULL,
codes_reseaux TEXT,
libelles_reseaux TEXT,
codes_supports TEXT NOT NULL,
libelles_supports TEXT NOT NULL,
codes_appel_taxons TEXT NOT NULL,
libelles_appel_taxons TEXT NOT NULL,
codes_indices TEXT,
libelles_indices TEXT,
latitude DOUBLE PRECISION NOT NULL,
longitude DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (code_station_hydrobio)
)
;


-- HYDROBIO_INDICES
-- URL: https://hubeau.eaufrance.fr/api/v1/hydrobio/indices.csv
-- Total records: ~1,097,634
CREATE TABLE IF NOT EXISTS hubeau.hydrobio_indices (
code_indice BIGINT NOT NULL,
libelle_indice TEXT NOT NULL,
code_station_hydrobio BIGINT NOT NULL,
libelle_station_hydrobio TEXT NOT NULL,
uri_station_hydrobio TEXT NOT NULL,
date_prelevement TEXT NOT NULL,
resultat_indice DOUBLE PRECISION NOT NULL,
unite_indice TEXT NOT NULL,
coordonnee_x DOUBLE PRECISION NOT NULL,
coordonnee_y DOUBLE PRECISION NOT NULL,
code_projection BIGINT NOT NULL,
code_cours_eau TEXT,
libelle_cours_eau TEXT,
uri_cours_eau TEXT,
code_masse_eau TEXT,
libelle_masse_eau TEXT,
uri_masse_eau TEXT,
code_sous_bassin TEXT,
libelle_sous_bassin TEXT,
code_bassin TEXT,
libelle_bassin TEXT,
code_commune BIGINT NOT NULL,
libelle_commune TEXT NOT NULL,
code_departement BIGINT NOT NULL,
libelle_departement TEXT NOT NULL,
code_region BIGINT NOT NULL,
libelle_region TEXT NOT NULL,
code_support BIGINT NOT NULL,
libelle_support TEXT NOT NULL,
code_qualification BIGINT NOT NULL,
libelle_qualification TEXT NOT NULL,
code_methode BIGINT NOT NULL,
libelle_methode TEXT NOT NULL,
libelle_accreditation TEXT NOT NULL,
code_prelevement BIGINT NOT NULL,
code_banque_reference TEXT NOT NULL,
code_operation_prelevement BIGINT NOT NULL,
latitude DOUBLE PRECISION NOT NULL,
longitude DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (code_indice, code_station_hydrobio)
)
WITH (
    fillfactor = 90,  -- Optimisation pour updates frequents (merge)
    autovacuum_vacuum_scale_factor = 0.05  -- Vacuum plus frequent
)
;


-- ECOULEMENT_STATIONS
-- URL: https://hubeau.eaufrance.fr/api/v1/ecoulement/stations.csv
-- Total records: ~3,548
CREATE TABLE IF NOT EXISTS hubeau.ecoulement_stations (
code_station TEXT NOT NULL,
libelle_station TEXT NOT NULL,
uri_station TEXT NOT NULL,
code_departement BIGINT NOT NULL,
libelle_departement TEXT NOT NULL,
code_commune BIGINT NOT NULL,
libelle_commune TEXT NOT NULL,
code_region BIGINT NOT NULL,
libelle_region TEXT NOT NULL,
code_bassin BIGINT NOT NULL,
libelle_bassin TEXT NOT NULL,
coordonnee_x_station DOUBLE PRECISION NOT NULL,
coordonnee_y_station DOUBLE PRECISION NOT NULL,
code_projection_station BIGINT NOT NULL,
libelle_projection_station TEXT NOT NULL,
code_epsg_station BIGINT NOT NULL,
code_cours_eau TEXT,
libelle_cours_eau TEXT,
uri_cours_eau TEXT,
etat_station TEXT NOT NULL,
date_maj_station TEXT NOT NULL,
latitude DOUBLE PRECISION NOT NULL,
longitude DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (code_station)
)
;


-- ECOULEMENT_OBSERVATIONS
-- URL: https://hubeau.eaufrance.fr/api/v1/ecoulement/observations.csv
-- Total records: ~344,899
CREATE TABLE IF NOT EXISTS hubeau.ecoulement_observations (
code_station TEXT NOT NULL,
libelle_station TEXT NOT NULL,
uri_station TEXT NOT NULL,
code_departement BIGINT NOT NULL,
libelle_departement TEXT NOT NULL,
code_commune BIGINT NOT NULL,
libelle_commune TEXT NOT NULL,
code_region BIGINT NOT NULL,
libelle_region TEXT NOT NULL,
code_bassin BIGINT NOT NULL,
libelle_bassin TEXT NOT NULL,
coordonnee_x_station DOUBLE PRECISION NOT NULL,
coordonnee_y_station DOUBLE PRECISION NOT NULL,
code_projection_station BIGINT NOT NULL,
libelle_projection_station TEXT NOT NULL,
code_cours_eau TEXT,
libelle_cours_eau TEXT,
uri_cours_eau TEXT,
code_campagne BIGINT NOT NULL,
code_reseau BIGINT NOT NULL,
libelle_reseau TEXT NOT NULL,
uri_reseau TEXT NOT NULL,
date_observation TEXT NOT NULL,
code_ecoulement TEXT NOT NULL,
libelle_ecoulement TEXT NOT NULL,
latitude DOUBLE PRECISION NOT NULL,
longitude DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (code_station, date_observation)
)
WITH (
    fillfactor = 90,  -- Optimisation pour updates frequents (merge)
    autovacuum_vacuum_scale_factor = 0.05  -- Vacuum plus frequent
)
;


-- ECOULEMENT_CAMPAGNES
-- URL: https://hubeau.eaufrance.fr/api/v1/ecoulement/campagnes.csv
-- Total records: ~9,606
CREATE TABLE IF NOT EXISTS hubeau.ecoulement_campagnes (
code_campagne BIGINT NOT NULL,
date_campagne TEXT NOT NULL,
nombre_modalite_ecoulement BIGINT NOT NULL,
code_type_campagne BIGINT NOT NULL,
libelle_type_campagne TEXT NOT NULL,
code_reseau BIGINT NOT NULL,
libelle_reseau TEXT NOT NULL,
uri_reseau TEXT NOT NULL,
code_departement TEXT NOT NULL,
libelle_departement TEXT NOT NULL,
    PRIMARY KEY (code_campagne)
)
;


-- PRELEVEMENTS_POINTS
-- URL: https://hubeau.eaufrance.fr/api/v1/prelevements/referentiel/points_prelevement.csv
-- Total records: ~186,754
CREATE TABLE IF NOT EXISTS hubeau.prelevements_points (
code_point_prelevement TEXT NOT NULL,
nom_point_prelevement TEXT NOT NULL,
date_exploitation_debut TEXT NOT NULL,
date_exploitation_fin DOUBLE PRECISION,
code_type_milieu TEXT NOT NULL,
libelle_type_milieu TEXT NOT NULL,
code_nature TEXT NOT NULL,
libelle_nature TEXT NOT NULL,
lieu_dit DOUBLE PRECISION,
commentaire DOUBLE PRECISION,
code_commune_insee BIGINT NOT NULL,
nom_commune TEXT NOT NULL,
code_departement BIGINT NOT NULL,
libelle_departement TEXT NOT NULL,
code_entite_hydro_cours_eau DOUBLE PRECISION,
uri_entite_hydro_cours_eau DOUBLE PRECISION,
code_entite_hydro_plan_eau DOUBLE PRECISION,
uri_entite_hydro_plan_eau DOUBLE PRECISION,
code_zone_hydro DOUBLE PRECISION,
uri_zone_hydro DOUBLE PRECISION,
code_mer_ocean DOUBLE PRECISION,
nappe_accompagnement BOOLEAN NOT NULL,
uri_bss_point_eau DOUBLE PRECISION,
code_ouvrage TEXT NOT NULL,
uri_ouvrage TEXT NOT NULL,
    PRIMARY KEY (code_point_prelevement)
)
;


-- PRELEVEMENTS_OUVRAGES
-- URL: https://hubeau.eaufrance.fr/api/v1/prelevements/referentiel/ouvrages.csv
-- Total records: ~168,208
CREATE TABLE IF NOT EXISTS hubeau.prelevements_ouvrages (
code_ouvrage TEXT NOT NULL,
nom_ouvrage TEXT NOT NULL,
id_local_ouvrage TEXT NOT NULL,
date_exploitation_debut TEXT NOT NULL,
date_exploitation_fin DOUBLE PRECISION,
code_precision_coord BIGINT NOT NULL,
libelle_precision_coord TEXT NOT NULL,
commentaire DOUBLE PRECISION,
code_commune_insee BIGINT NOT NULL,
nom_commune TEXT NOT NULL,
code_departement BIGINT NOT NULL,
libelle_departement TEXT NOT NULL,
code_type_milieu TEXT NOT NULL,
libelle_type_milieu TEXT NOT NULL,
code_entite_hydro_cours_eau DOUBLE PRECISION,
uri_entite_hydro_cours_eau DOUBLE PRECISION,
code_entite_hydro_plan_eau DOUBLE PRECISION,
uri_entite_hydro_plan_eau DOUBLE PRECISION,
code_mer_ocean DOUBLE PRECISION,
ressource_cont_non_referencee BOOLEAN NOT NULL,
ressource_cont_non_referencee_info DOUBLE PRECISION,
code_point_referent TEXT NOT NULL,
code_bdlisa DOUBLE PRECISION,
uri_bdlisa DOUBLE PRECISION,
longitude DOUBLE PRECISION NOT NULL,
latitude DOUBLE PRECISION NOT NULL,
uri_ouvrage TEXT NOT NULL,
    PRIMARY KEY (code_ouvrage)
)
;


-- PIEZOMETRY_CHRONIQUES
-- URL: https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/chroniques.csv
-- Total records: ~50,000,000
CREATE TABLE IF NOT EXISTS hubeau.piezometry_chroniques (
code_bss TEXT NOT NULL,
date_mesure DATE NOT NULL,
niveau_nappe_ngf DOUBLE PRECISION,
mode_obtention TEXT,
statut TEXT,
qualification TEXT,
    PRIMARY KEY (code_bss, date_mesure)
)
WITH (
    fillfactor = 90,  -- Optimisation pour updates frequents (merge)
    autovacuum_vacuum_scale_factor = 0.05  -- Vacuum plus frequent
)
;


-- TEMPERATURE_CHRONIQUES
-- URL: https://hubeau.eaufrance.fr/api/v1/temperature/chronique.csv
-- Total records: ~49,315,252
CREATE TABLE IF NOT EXISTS hubeau.temperature_chroniques (
code_station TEXT NOT NULL,
date_mesure_temp DATE NOT NULL,
code_parametre TEXT NOT NULL,
heure_mesure_temp TIME,
resultat DOUBLE PRECISION,
code_qualification TEXT,
    PRIMARY KEY (code_station, date_mesure_temp, code_parametre)
)
WITH (
    fillfactor = 90,  -- Optimisation pour updates frequents (merge)
    autovacuum_vacuum_scale_factor = 0.05  -- Vacuum plus frequent
)
;


-- HYDROBIO_TAXONS
-- URL: https://hubeau.eaufrance.fr/api/v1/hydrobio/taxons.csv
-- Total records: ~10,767,254
CREATE TABLE IF NOT EXISTS hubeau.hydrobio_taxons (
code_station_hydrobio TEXT NOT NULL,
date_prelevement DATE NOT NULL,
code_appel_taxon TEXT NOT NULL,
code_support TEXT,
resultat DOUBLE PRECISION,
    PRIMARY KEY (code_station_hydrobio, date_prelevement, code_appel_taxon)
)
WITH (
    fillfactor = 90,  -- Optimisation pour updates frequents (merge)
    autovacuum_vacuum_scale_factor = 0.05  -- Vacuum plus frequent
)
;


-- PRELEVEMENTS_CHRONIQUES
-- URL: https://hubeau.eaufrance.fr/api/v1/prelevements/chroniques.csv
-- Total records: ~1,171,702
CREATE TABLE IF NOT EXISTS hubeau.prelevements_chroniques (
code_ouvrage TEXT NOT NULL,
annee INTEGER NOT NULL,
code_usage TEXT NOT NULL,
volume DOUBLE PRECISION,
libelle_usage TEXT,
    PRIMARY KEY (code_ouvrage, annee, code_usage)
)
WITH (
    fillfactor = 90,  -- Optimisation pour updates frequents (merge)
    autovacuum_vacuum_scale_factor = 0.05  -- Vacuum plus frequent
)
;


-- ========================================================
-- INDEXES RECOMMANDES
-- ========================================================

-- Indexes pour piezometry_stations
CREATE INDEX IF NOT EXISTS idx_piezometry_stations_date_fin_mesure ON hubeau.piezometry_stations(date_fin_mesure);
CREATE INDEX IF NOT EXISTS idx_piezometry_stations_date_maj ON hubeau.piezometry_stations(date_maj);
CREATE INDEX IF NOT EXISTS idx_piezometry_stations_date_debut_mesure ON hubeau.piezometry_stations(date_debut_mesure);
CREATE INDEX IF NOT EXISTS idx_piezometry_stations_code_departement ON hubeau.piezometry_stations(code_departement);

-- Indexes pour quality_groundwater_stations
CREATE INDEX IF NOT EXISTS idx_quality_groundwater_stations_date_fin_mesure ON hubeau.quality_groundwater_stations(date_fin_mesure);
CREATE INDEX IF NOT EXISTS idx_quality_groundwater_stations_date_debut_mesure ON hubeau.quality_groundwater_stations(date_debut_mesure);

-- Indexes pour quality_groundwater_analyses
CREATE INDEX IF NOT EXISTS idx_quality_groundwater_analyses_code_region ON hubeau.quality_groundwater_analyses(code_region);

-- Indexes pour quality_rivers_stations
CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_code_departement ON hubeau.quality_rivers_stations(code_departement);
CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_code_commune ON hubeau.quality_rivers_stations(code_commune);
CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_date_creation ON hubeau.quality_rivers_stations(date_creation);
CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_date_arret ON hubeau.quality_rivers_stations(date_arret);
CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_date_maj_information ON hubeau.quality_rivers_stations(date_maj_information);
CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_code_region ON hubeau.quality_rivers_stations(code_region);

-- Indexes pour quality_rivers_analyses
CREATE INDEX IF NOT EXISTS idx_quality_rivers_analyses_date_prelevement ON hubeau.quality_rivers_analyses(date_prelevement);
CREATE INDEX IF NOT EXISTS idx_quality_rivers_analyses_date_maj_analyse ON hubeau.quality_rivers_analyses(date_maj_analyse);
CREATE INDEX IF NOT EXISTS idx_brin_quality_rivers_analyses_date_prelevement ON hubeau.quality_rivers_analyses USING BRIN (date_prelevement);

-- Indexes pour quality_rivers_conditions
CREATE INDEX IF NOT EXISTS idx_quality_rivers_conditions_date_maj ON hubeau.quality_rivers_conditions(date_maj);
CREATE INDEX IF NOT EXISTS idx_quality_rivers_conditions_date_mesure ON hubeau.quality_rivers_conditions(date_mesure);
CREATE INDEX IF NOT EXISTS idx_brin_quality_rivers_conditions_date_mesure ON hubeau.quality_rivers_conditions USING BRIN (date_mesure);

-- Indexes pour quality_rivers_operations
CREATE INDEX IF NOT EXISTS idx_quality_rivers_operations_date_fin ON hubeau.quality_rivers_operations(date_fin);
CREATE INDEX IF NOT EXISTS idx_quality_rivers_operations_date_prelevement ON hubeau.quality_rivers_operations(date_prelevement);

-- Indexes pour temperature_stations
CREATE INDEX IF NOT EXISTS idx_temperature_stations_code_region ON hubeau.temperature_stations(code_region);
CREATE INDEX IF NOT EXISTS idx_temperature_stations_code_commune ON hubeau.temperature_stations(code_commune);
CREATE INDEX IF NOT EXISTS idx_temperature_stations_date_maj_infos ON hubeau.temperature_stations(date_maj_infos);
CREATE INDEX IF NOT EXISTS idx_temperature_stations_code_departement ON hubeau.temperature_stations(code_departement);

-- Indexes pour hydrometry_sites
CREATE INDEX IF NOT EXISTS idx_hydrometry_sites_date_premiere_donnee_dispo_site ON hubeau.hydrometry_sites(date_premiere_donnee_dispo_site);
CREATE INDEX IF NOT EXISTS idx_hydrometry_sites_code_region ON hubeau.hydrometry_sites(code_region);
CREATE INDEX IF NOT EXISTS idx_hydrometry_sites_date_maj_site ON hubeau.hydrometry_sites(date_maj_site);
CREATE INDEX IF NOT EXISTS idx_hydrometry_sites_code_departement ON hubeau.hydrometry_sites(code_departement);

-- Indexes pour hydrometry_stations
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_date_ouverture_station ON hubeau.hydrometry_stations(date_ouverture_station);
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_code_departement ON hubeau.hydrometry_stations(code_departement);
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_date_maj_ref_alti_station ON hubeau.hydrometry_stations(date_maj_ref_alti_station);
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_date_fermeture_station ON hubeau.hydrometry_stations(date_fermeture_station);
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_code_region ON hubeau.hydrometry_stations(code_region);
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_date_activation_ref_alti_station ON hubeau.hydrometry_stations(date_activation_ref_alti_station);
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_date_debut_ref_alti_station ON hubeau.hydrometry_stations(date_debut_ref_alti_station);
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_date_maj_station ON hubeau.hydrometry_stations(date_maj_station);

-- Indexes pour hydrometry_obs_elab
CREATE INDEX IF NOT EXISTS idx_hydrometry_obs_elab_date_prod ON hubeau.hydrometry_obs_elab(date_prod);
CREATE INDEX IF NOT EXISTS idx_brin_hydrometry_obs_elab_date_prod ON hubeau.hydrometry_obs_elab USING BRIN (date_prod);

-- Indexes pour hydrobio_stations
CREATE INDEX IF NOT EXISTS idx_hydrobio_stations_code_region ON hubeau.hydrobio_stations(code_region);
CREATE INDEX IF NOT EXISTS idx_hydrobio_stations_code_commune ON hubeau.hydrobio_stations(code_commune);
CREATE INDEX IF NOT EXISTS idx_hydrobio_stations_code_departement ON hubeau.hydrobio_stations(code_departement);

-- Indexes pour hydrobio_indices
CREATE INDEX IF NOT EXISTS idx_hydrobio_indices_code_region ON hubeau.hydrobio_indices(code_region);
CREATE INDEX IF NOT EXISTS idx_hydrobio_indices_code_commune ON hubeau.hydrobio_indices(code_commune);
CREATE INDEX IF NOT EXISTS idx_hydrobio_indices_date_prelevement ON hubeau.hydrobio_indices(date_prelevement);
CREATE INDEX IF NOT EXISTS idx_hydrobio_indices_code_departement ON hubeau.hydrobio_indices(code_departement);

-- Indexes pour ecoulement_stations
CREATE INDEX IF NOT EXISTS idx_ecoulement_stations_code_region ON hubeau.ecoulement_stations(code_region);
CREATE INDEX IF NOT EXISTS idx_ecoulement_stations_date_maj_station ON hubeau.ecoulement_stations(date_maj_station);
CREATE INDEX IF NOT EXISTS idx_ecoulement_stations_code_commune ON hubeau.ecoulement_stations(code_commune);
CREATE INDEX IF NOT EXISTS idx_ecoulement_stations_code_departement ON hubeau.ecoulement_stations(code_departement);

-- Indexes pour ecoulement_observations
CREATE INDEX IF NOT EXISTS idx_ecoulement_observations_code_region ON hubeau.ecoulement_observations(code_region);
CREATE INDEX IF NOT EXISTS idx_ecoulement_observations_code_commune ON hubeau.ecoulement_observations(code_commune);
CREATE INDEX IF NOT EXISTS idx_ecoulement_observations_code_departement ON hubeau.ecoulement_observations(code_departement);
CREATE INDEX IF NOT EXISTS idx_ecoulement_observations_station_date ON hubeau.ecoulement_observations(code_station, date_observation DESC);

-- Indexes pour ecoulement_campagnes
CREATE INDEX IF NOT EXISTS idx_ecoulement_campagnes_date_campagne ON hubeau.ecoulement_campagnes(date_campagne);
CREATE INDEX IF NOT EXISTS idx_ecoulement_campagnes_code_departement ON hubeau.ecoulement_campagnes(code_departement);

-- Indexes pour prelevements_points
CREATE INDEX IF NOT EXISTS idx_prelevements_points_date_exploitation_fin ON hubeau.prelevements_points(date_exploitation_fin);
CREATE INDEX IF NOT EXISTS idx_prelevements_points_date_exploitation_debut ON hubeau.prelevements_points(date_exploitation_debut);
CREATE INDEX IF NOT EXISTS idx_prelevements_points_code_departement ON hubeau.prelevements_points(code_departement);

-- Indexes pour prelevements_ouvrages
CREATE INDEX IF NOT EXISTS idx_prelevements_ouvrages_date_exploitation_fin ON hubeau.prelevements_ouvrages(date_exploitation_fin);
CREATE INDEX IF NOT EXISTS idx_prelevements_ouvrages_date_exploitation_debut ON hubeau.prelevements_ouvrages(date_exploitation_debut);
CREATE INDEX IF NOT EXISTS idx_prelevements_ouvrages_code_departement ON hubeau.prelevements_ouvrages(code_departement);

-- Indexes pour piezometry_chroniques
CREATE INDEX IF NOT EXISTS idx_piezometry_chroniques_station_date ON hubeau.piezometry_chroniques(code_bss, date_mesure DESC);

-- Indexes pour temperature_chroniques
CREATE INDEX IF NOT EXISTS idx_temperature_chroniques_station_date ON hubeau.temperature_chroniques(code_station, date_mesure_temp DESC);

-- Indexes pour prelevements_chroniques
CREATE INDEX IF NOT EXISTS idx_prelevements_chroniques_station_date ON hubeau.prelevements_chroniques(code_ouvrage, annee DESC);


-- ========================================================
-- PERMISSIONS
-- ========================================================

GRANT USAGE ON SCHEMA hubeau TO hubeau_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA hubeau TO hubeau_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA hubeau GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO hubeau_user;
