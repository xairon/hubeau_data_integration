-- ====================================
-- INDEX POSTGRESQL POUR PERFORMANCE
-- ====================================
--
-- Ce script crée les index optimisés pour les requêtes fréquentes
-- sur les tables Hub'Eau.
--
-- Exécution:
--   psql -U postgres -d postgres -f 01_create_indexes.sql
--
-- Ou via Adminer/PgAdmin en copiant le contenu
--
-- Durée estimée: 5-15 minutes selon volume de données
-- ====================================

\timing on
SET search_path TO hubeau, public;

-- ====================================
-- PIÉZOMÉTRIE (NAPPES PHRÉATIQUES)
-- ====================================

-- Stations: Index sur code unique
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_piezo_stations_code
ON piezometry_stations(code_station);

-- Stations: Index spatial (pour requêtes géographiques)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_piezo_stations_coords
ON piezometry_stations(latitude, longitude)
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- Chroniques: Index sur code BSS (filtrage par station)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_piezo_chroniques_bss
ON piezometry_chroniques(code_bss);

-- Chroniques: Index sur date (filtrage temporel)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_piezo_chroniques_date
ON piezometry_chroniques(timestamp_mesure);

-- Chroniques: Index composite (station + date) - CRITIQUE pour performance
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_piezo_chroniques_bss_date
ON piezometry_chroniques(code_bss, timestamp_mesure DESC);

-- Chroniques: Index partiel sur mesures récentes (< 1 an)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_piezo_chroniques_recent
ON piezometry_chroniques(code_bss, timestamp_mesure)
WHERE timestamp_mesure > (CURRENT_DATE - INTERVAL '1 year');

-- ====================================
-- HYDROMÉTRIE (DÉBITS/HAUTEURS)
-- ====================================

-- Stations: Index sur code unique
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hydro_stations_code
ON hydrometry_stations(code_station);

-- Stations: Index spatial
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hydro_stations_coords
ON hydrometry_stations(latitude_station, longitude_station)
WHERE latitude_station IS NOT NULL AND longitude_station IS NOT NULL;

-- Sites: Index sur code unique
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hydro_sites_code
ON hydrometry_sites(code_site);

-- Observations élaborées: Index sur station
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hydro_obs_station
ON hydrometry_obs_elab(code_entite);

-- Observations élaborées: Index sur date
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hydro_obs_date
ON hydrometry_obs_elab(date_obs);

-- Observations élaborées: Index composite (station + date)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hydro_obs_station_date
ON hydrometry_obs_elab(code_entite, date_obs DESC);

-- ====================================
-- QUALITÉ DES EAUX - COURS D'EAU
-- ====================================

-- Stations: Index sur code unique
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_quality_rivers_stations_code
ON quality_rivers_stations(code_station);

-- Stations: Index spatial
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_quality_rivers_stations_coords
ON quality_rivers_stations(latitude, longitude)
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- Analyses: Index sur station
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_quality_rivers_analyses_station
ON quality_rivers_analyses(code_station);

-- Analyses: Index sur date prélèvement
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_quality_rivers_analyses_date
ON quality_rivers_analyses(date_prelevement);

-- Analyses: Index sur paramètre (ex: nitrates, phosphates)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_quality_rivers_analyses_param
ON quality_rivers_analyses(code_parametre);

-- Analyses: Index composite (station + date + paramètre) - OPTIMAL
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_quality_rivers_analyses_full
ON quality_rivers_analyses(code_station, date_prelevement DESC, code_parametre);

-- Opérations: Index sur code opération
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_quality_rivers_operations_code
ON quality_rivers_operations(code_operation);

-- Conditions: Index sur code condition
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_quality_rivers_conditions_code
ON quality_rivers_conditions(code_condition);

-- ====================================
-- QUALITÉ DES EAUX - NAPPES
-- ====================================

-- Stations: Index sur code BSS
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_quality_groundwater_stations_bss
ON quality_groundwater_stations(code_bss);

-- Stations: Index spatial
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_quality_groundwater_stations_coords
ON quality_groundwater_stations(latitude, longitude)
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- Analyses: Index sur BSS
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_quality_groundwater_analyses_bss
ON quality_groundwater_analyses(bss_id);

-- Analyses: Index sur date
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_quality_groundwater_analyses_date
ON quality_groundwater_analyses(date_analyse);

-- Analyses: Index sur paramètre
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_quality_groundwater_analyses_param
ON quality_groundwater_analyses(code_parametre);

-- Analyses: Index composite
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_quality_groundwater_analyses_full
ON quality_groundwater_analyses(bss_id, date_analyse DESC, code_parametre);

-- ====================================
-- TEMPÉRATURE CONTINUE
-- ====================================

-- Stations: Index sur code
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_temperature_stations_code
ON temperature_stations(code_station);

-- Stations: Index spatial
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_temperature_stations_coords
ON temperature_stations(latitude, longitude)
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- Chroniques: Index sur station
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_temperature_chroniques_station
ON temperature_chroniques(code_station);

-- Chroniques: Index sur date
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_temperature_chroniques_date
ON temperature_chroniques(date_mesure);

-- Chroniques: Index composite
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_temperature_chroniques_full
ON temperature_chroniques(code_station, date_mesure DESC);

-- ====================================
-- ÉCOULEMENT (ONDE)
-- ====================================

-- Stations: Index sur code
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ecoulement_stations_code
ON ecoulement_stations(code_station);

-- Stations: Index spatial
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ecoulement_stations_coords
ON ecoulement_stations(latitude, longitude)
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- Observations: Index sur station
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ecoulement_observations_station
ON ecoulement_observations(code_station);

-- Observations: Index sur date
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ecoulement_observations_date
ON ecoulement_observations(date_observation);

-- Observations: Index composite
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ecoulement_observations_full
ON ecoulement_observations(code_station, date_observation DESC);

-- ====================================
-- HYDROBIOLOGIE
-- ====================================

-- Stations: Index sur code
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hydrobio_stations_code
ON hydrobio_stations(code_station_hydrobio);

-- Stations: Index spatial
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hydrobio_stations_coords
ON hydrobio_stations(latitude, longitude)
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- Indices: Index sur station
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hydrobio_indices_station
ON hydrobio_indices(code_station_hydrobio);

-- Indices: Index sur date
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hydrobio_indices_date
ON hydrobio_indices(date_prelevement);

-- Taxons: Index sur station
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hydrobio_taxons_station
ON hydrobio_taxons(code_station_hydrobio);

-- Taxons: Index sur date
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hydrobio_taxons_date
ON hydrobio_taxons(date_prelevement);

-- ====================================
-- PRÉLÈVEMENTS
-- ====================================

-- Ouvrages: Index sur code
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_prelevements_ouvrages_code
ON prelevements_ouvrages(code_ouvrage);

-- Points: Index sur code point
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_prelevements_points_code
ON prelevements_points(code_point_prelevement);

-- Chroniques: Index sur ouvrage
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_prelevements_chroniques_ouvrage
ON prelevements_chroniques(code_ouvrage);

-- Chroniques: Index sur année
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_prelevements_chroniques_annee
ON prelevements_chroniques(annee);

-- Chroniques: Index composite
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_prelevements_chroniques_full
ON prelevements_chroniques(code_ouvrage, annee DESC);

-- ====================================
-- INDEX DLT METADATA (PERFORMANCE DAGSTER UI)
-- ====================================

-- Index sur table des chargements DLT
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dlt_loads_inserted
ON _dlt_loads(inserted_at DESC);

-- ====================================
-- STATISTIQUES POSTGRESQL
-- ====================================

-- Mettre à jour les statistiques pour l'optimiseur de requêtes
ANALYZE;

-- ====================================
-- RÉSUMÉ DES INDEX CRÉÉS
-- ====================================

SELECT
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_indexes
JOIN pg_class ON indexrelid = pg_class.oid
WHERE schemaname = 'hubeau'
ORDER BY tablename, indexname;

\echo '✅ Index créés avec succès!'
\echo 'Note: Les index CONCURRENTLY permettent de créer les index sans bloquer les écritures.'
\echo 'Durée de création dépend du volume de données (quelques minutes à quelques heures).'
