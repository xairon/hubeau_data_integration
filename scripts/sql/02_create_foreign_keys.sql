-- ====================================
-- CONTRAINTES FOREIGN KEYS
-- ====================================
--
-- Ce script crée les contraintes de clés étrangères pour garantir
-- l'intégrité référentielle entre les tables Hub'Eau.
--
-- IMPORTANT: Ce script doit être exécuté APRÈS l'ingestion initiale
-- des données, car il va valider que toutes les références existent.
--
-- Exécution:
--   psql -U postgres -d postgres -f 02_create_foreign_keys.sql
--
-- En cas d'erreur:
--   - Vérifier que les stations de référence sont bien chargées
--   - Supprimer les lignes orphelines avant d'ajouter les contraintes
-- ====================================

\timing on
SET search_path TO hubeau, public;

-- ====================================
-- VÉRIFICATION PRÉALABLE - DÉTECTER ORPHELINS
-- ====================================

\echo '====================================';
\echo 'VÉRIFICATION DES DONNÉES ORPHELINES';
\echo '====================================';
\echo '';

-- Piézométrie: Chroniques sans station
\echo 'Piézométrie - Chroniques sans station:';
SELECT COUNT(*) AS orphelins_piezo_chroniques
FROM piezometry_chroniques pc
LEFT JOIN piezometry_stations ps ON pc.code_bss = ps.code_station
WHERE ps.code_station IS NULL;

-- Hydrométrie: Observations sans station
\echo 'Hydrométrie - Observations sans station:';
SELECT COUNT(*) AS orphelins_hydro_obs
FROM hydrometry_obs_elab ho
LEFT JOIN hydrometry_stations hs ON ho.code_entite = hs.code_station
WHERE hs.code_station IS NULL;

-- Qualité rivières: Analyses sans station
\echo 'Qualité rivières - Analyses sans station:';
SELECT COUNT(*) AS orphelins_quality_rivers_analyses
FROM quality_rivers_analyses qa
LEFT JOIN quality_rivers_stations qs ON qa.code_station = qs.code_station
WHERE qs.code_station IS NULL;

-- Qualité nappes: Analyses sans station
\echo 'Qualité nappes - Analyses sans station:';
SELECT COUNT(*) AS orphelins_quality_groundwater_analyses
FROM quality_groundwater_analyses qa
LEFT JOIN quality_groundwater_stations qs ON qa.bss_id = qs.code_bss
WHERE qs.code_bss IS NULL;

-- Température: Chroniques sans station
\echo 'Température - Chroniques sans station:';
SELECT COUNT(*) AS orphelins_temperature_chroniques
FROM temperature_chroniques tc
LEFT JOIN temperature_stations ts ON tc.code_station = ts.code_station
WHERE ts.code_station IS NULL;

-- Écoulement: Observations sans station
\echo 'Écoulement - Observations sans station:';
SELECT COUNT(*) AS orphelins_ecoulement_observations
FROM ecoulement_observations eo
LEFT JOIN ecoulement_stations es ON eo.code_station = es.code_station
WHERE es.code_station IS NULL;

-- Hydrobiologie indices: Sans station
\echo 'Hydrobiologie - Indices sans station:';
SELECT COUNT(*) AS orphelins_hydrobio_indices
FROM hydrobio_indices hi
LEFT JOIN hydrobio_stations hs ON hi.code_station_hydrobio = hs.code_station_hydrobio
WHERE hs.code_station_hydrobio IS NULL;

-- Hydrobiologie taxons: Sans station
\echo 'Hydrobiologie - Taxons sans station:';
SELECT COUNT(*) AS orphelins_hydrobio_taxons
FROM hydrobio_taxons ht
LEFT JOIN hydrobio_stations hs ON ht.code_station_hydrobio = hs.code_station_hydrobio
WHERE hs.code_station_hydrobio IS NULL;

-- Prélèvements chroniques: Sans ouvrage
\echo 'Prélèvements - Chroniques sans ouvrage:';
SELECT COUNT(*) AS orphelins_prelevements_chroniques
FROM prelevements_chroniques pc
LEFT JOIN prelevements_ouvrages po ON pc.code_ouvrage = po.code_ouvrage
WHERE po.code_ouvrage IS NULL;

\echo '';
\echo 'Si des orphelins sont détectés, il faut soit:';
\echo '  1. Charger les stations manquantes';
\echo '  2. Supprimer les lignes orphelines';
\echo '  3. Utiliser ON DELETE CASCADE pour suppression auto';
\echo '';

-- ====================================
-- OPTION: SUPPRIMER LES ORPHELINS (DÉCOMMENTER SI BESOIN)
-- ====================================

-- ATTENTION: Ces commandes suppriment les données orphelines!
-- Décommenter uniquement si vous êtes sûr de vouloir perdre ces données.

-- DELETE FROM piezometry_chroniques pc
-- WHERE NOT EXISTS (
--     SELECT 1 FROM piezometry_stations ps WHERE ps.code_station = pc.code_bss
-- );

-- DELETE FROM hydrometry_obs_elab ho
-- WHERE NOT EXISTS (
--     SELECT 1 FROM hydrometry_stations hs WHERE hs.code_station = ho.code_entite
-- );

-- DELETE FROM quality_rivers_analyses qa
-- WHERE NOT EXISTS (
--     SELECT 1 FROM quality_rivers_stations qs WHERE qs.code_station = qa.code_station
-- );

-- (etc. pour les autres tables)

-- ====================================
-- CRÉATION DES FOREIGN KEYS
-- ====================================

\echo '====================================';
\echo 'CRÉATION DES CONTRAINTES FOREIGN KEYS';
\echo '====================================';
\echo '';

-- PIÉZOMÉTRIE
\echo 'Piézométrie...';
ALTER TABLE piezometry_chroniques
ADD CONSTRAINT fk_piezo_chroniques_station
FOREIGN KEY (code_bss)
REFERENCES piezometry_stations(code_station)
ON DELETE CASCADE  -- Si station supprimée, supprimer chroniques associées
ON UPDATE CASCADE;

-- HYDROMÉTRIE
\echo 'Hydrométrie...';
ALTER TABLE hydrometry_obs_elab
ADD CONSTRAINT fk_hydro_obs_station
FOREIGN KEY (code_entite)
REFERENCES hydrometry_stations(code_station)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- QUALITÉ COURS D'EAU
\echo 'Qualité cours d''eau...';
ALTER TABLE quality_rivers_analyses
ADD CONSTRAINT fk_quality_rivers_analyses_station
FOREIGN KEY (code_station)
REFERENCES quality_rivers_stations(code_station)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- QUALITÉ NAPPES
\echo 'Qualité nappes...';
ALTER TABLE quality_groundwater_analyses
ADD CONSTRAINT fk_quality_groundwater_analyses_station
FOREIGN KEY (bss_id)
REFERENCES quality_groundwater_stations(code_bss)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- TEMPÉRATURE
\echo 'Température...';
ALTER TABLE temperature_chroniques
ADD CONSTRAINT fk_temperature_chroniques_station
FOREIGN KEY (code_station)
REFERENCES temperature_stations(code_station)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- ÉCOULEMENT
\echo 'Écoulement...';
ALTER TABLE ecoulement_observations
ADD CONSTRAINT fk_ecoulement_observations_station
FOREIGN KEY (code_station)
REFERENCES ecoulement_stations(code_station)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- HYDROBIOLOGIE INDICES
\echo 'Hydrobiologie indices...';
ALTER TABLE hydrobio_indices
ADD CONSTRAINT fk_hydrobio_indices_station
FOREIGN KEY (code_station_hydrobio)
REFERENCES hydrobio_stations(code_station_hydrobio)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- HYDROBIOLOGIE TAXONS
\echo 'Hydrobiologie taxons...';
ALTER TABLE hydrobio_taxons
ADD CONSTRAINT fk_hydrobio_taxons_station
FOREIGN KEY (code_station_hydrobio)
REFERENCES hydrobio_stations(code_station_hydrobio)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- PRÉLÈVEMENTS
\echo 'Prélèvements...';
ALTER TABLE prelevements_chroniques
ADD CONSTRAINT fk_prelevements_chroniques_ouvrage
FOREIGN KEY (code_ouvrage)
REFERENCES prelevements_ouvrages(code_ouvrage)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- ====================================
-- VÉRIFICATION DES CONTRAINTES CRÉÉES
-- ====================================

\echo '';
\echo '====================================';
\echo 'CONTRAINTES CRÉÉES:';
\echo '====================================';

SELECT
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name,
    rc.delete_rule,
    rc.update_rule
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
    AND ccu.table_schema = tc.table_schema
JOIN information_schema.referential_constraints AS rc
    ON rc.constraint_name = tc.constraint_name
    AND rc.constraint_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema = 'hubeau'
ORDER BY tc.table_name;

\echo '';
\echo '✅ Foreign keys créées avec succès!';
\echo '';
\echo 'Bénéfices:';
\echo '  - Intégrité référentielle garantie';
\echo '  - Suppression en cascade automatique';
\echo '  - Erreurs si insertion de données orphelines';
\echo '';
\echo 'Note: Les foreign keys peuvent ralégir légèrement les INSERT/UPDATE';
\echo 'mais garantissent la cohérence des données.';
