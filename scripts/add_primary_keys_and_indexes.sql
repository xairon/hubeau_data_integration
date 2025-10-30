-- ============================================================================
-- SCRIPT: Ajout de clés primaires et indexes pour les tables Hub'Eau
-- ============================================================================
-- Ce script ajoute les PRIMARY KEY et indexes manquants sur les tables existantes
-- Généré automatiquement depuis TABLE_PK_MAPPING
-- Date: 2025-10-30
-- ============================================================================

\timing on
\set ON_ERROR_STOP off

SET search_path TO hubeau;

-- ============================================================================
-- PARTIE 1: AJOUT DES PRIMARY KEYS
-- ============================================================================

-- Écoulement
ALTER TABLE ecoulement_campagnes DROP CONSTRAINT IF EXISTS ecoulement_campagnes_pkey CASCADE;
ALTER TABLE ecoulement_campagnes ADD PRIMARY KEY (code_campagne);

ALTER TABLE ecoulement_stations DROP CONSTRAINT IF EXISTS ecoulement_stations_pkey CASCADE;
ALTER TABLE ecoulement_stations ADD PRIMARY KEY (code_station);

-- Hydrobiologie
ALTER TABLE hydrobio_stations DROP CONSTRAINT IF EXISTS hydrobio_stations_pkey CASCADE;
ALTER TABLE hydrobio_stations ADD PRIMARY KEY (code_station_hydrobio);

-- Hydrométrie
ALTER TABLE hydrometry_sites DROP CONSTRAINT IF EXISTS hydrometry_sites_pkey CASCADE;
ALTER TABLE hydrometry_sites ADD PRIMARY KEY (code_site);

ALTER TABLE hydrometry_stations DROP CONSTRAINT IF EXISTS hydrometry_stations_pkey CASCADE;
ALTER TABLE hydrometry_stations ADD PRIMARY KEY (code_station);

-- Piézométrie
ALTER TABLE piezometry_stations DROP CONSTRAINT IF EXISTS piezometry_stations_pkey CASCADE;
ALTER TABLE piezometry_stations ADD PRIMARY KEY (code_bss);

-- Prélèvements
ALTER TABLE prelevements_ouvrages DROP CONSTRAINT IF EXISTS prelevements_ouvrages_pkey CASCADE;
ALTER TABLE prelevements_ouvrages ADD PRIMARY KEY (code_ouvrage);

ALTER TABLE prelevements_points DROP CONSTRAINT IF EXISTS prelevements_points_pkey CASCADE;
ALTER TABLE prelevements_points ADD PRIMARY KEY (code_point_prelevement);

-- Qualité des eaux souterraines
ALTER TABLE quality_groundwater_stations DROP CONSTRAINT IF EXISTS quality_groundwater_stations_pkey CASCADE;
ALTER TABLE quality_groundwater_stations ADD PRIMARY KEY (code_bss);

-- Qualité des cours d'eau
ALTER TABLE quality_rivers_stations DROP CONSTRAINT IF EXISTS quality_rivers_stations_pkey CASCADE;
ALTER TABLE quality_rivers_stations ADD PRIMARY KEY (code_station);

-- Température
ALTER TABLE temperature_stations DROP CONSTRAINT IF EXISTS temperature_stations_pkey CASCADE;
ALTER TABLE temperature_stations ADD PRIMARY KEY (code_station);

-- ============================================================================
-- PARTIE 2: INDEXES SUR COLONNES FRÉQUEMMENT UTILISÉES
-- ============================================================================

-- Index sur les départements (filtrage géographique fréquent)
CREATE INDEX IF NOT EXISTS idx_ecoulement_stations_dept ON ecoulement_stations(code_departement);
CREATE INDEX IF NOT EXISTS idx_hydrobio_stations_dept ON hydrobio_stations(code_departement);
CREATE INDEX IF NOT EXISTS idx_hydrometry_sites_dept ON hydrometry_sites(code_departement);
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_dept ON hydrometry_stations(code_departement);
CREATE INDEX IF NOT EXISTS idx_piezometry_stations_dept ON piezometry_stations(code_departement);
CREATE INDEX IF NOT EXISTS idx_prelevements_ouvrages_dept ON prelevements_ouvrages(code_departement);
CREATE INDEX IF NOT EXISTS idx_prelevements_points_dept ON prelevements_points(code_departement);
CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_dept ON quality_rivers_stations(code_departement);
CREATE INDEX IF NOT EXISTS idx_temperature_stations_dept ON temperature_stations(code_departement);

-- Index sur les dates (requêtes temporelles fréquentes)
CREATE INDEX IF NOT EXISTS idx_ecoulement_campagnes_date ON ecoulement_campagnes(date_campagne);
CREATE INDEX IF NOT EXISTS idx_hydrometry_sites_date_maj ON hydrometry_sites(date_maj_site);
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_date_maj ON hydrometry_stations(date_maj_station);
CREATE INDEX IF NOT EXISTS idx_piezometry_stations_date_maj ON piezometry_stations(date_maj);
CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_date_maj ON quality_rivers_stations(date_maj_information);
CREATE INDEX IF NOT EXISTS idx_temperature_stations_date_maj ON temperature_stations(date_maj_infos);

-- Index sur les communes (requêtes locales fréquentes)
CREATE INDEX IF NOT EXISTS idx_ecoulement_stations_commune ON ecoulement_stations(code_commune);
CREATE INDEX IF NOT EXISTS idx_hydrobio_stations_commune ON hydrobio_stations(code_commune);
CREATE INDEX IF NOT EXISTS idx_hydrometry_sites_commune ON hydrometry_sites(code_commune_site);
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_commune ON hydrometry_stations(code_commune_station);
CREATE INDEX IF NOT EXISTS idx_piezometry_stations_commune ON piezometry_stations(code_commune_insee);
CREATE INDEX IF NOT EXISTS idx_prelevements_ouvrages_commune ON prelevements_ouvrages(code_commune_insee);
CREATE INDEX IF NOT EXISTS idx_prelevements_points_commune ON prelevements_points(code_commune_insee);
CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_commune ON quality_rivers_stations(code_commune);
CREATE INDEX IF NOT EXISTS idx_temperature_stations_commune ON temperature_stations(code_commune);

-- Index sur les cours d'eau (jointures fréquentes)
CREATE INDEX IF NOT EXISTS idx_ecoulement_stations_cours_eau ON ecoulement_stations(code_cours_eau);
CREATE INDEX IF NOT EXISTS idx_hydrobio_stations_cours_eau ON hydrobio_stations(code_cours_eau);
CREATE INDEX IF NOT EXISTS idx_hydrometry_sites_cours_eau ON hydrometry_sites(code_cours_eau);
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_cours_eau ON hydrometry_stations(code_cours_eau);
CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_cours_eau ON quality_rivers_stations(code_cours_eau);
CREATE INDEX IF NOT EXISTS idx_temperature_stations_cours_eau ON temperature_stations(code_cours_eau);

-- Index sur les régions (agrégations par région)
CREATE INDEX IF NOT EXISTS idx_ecoulement_stations_region ON ecoulement_stations(code_region);
CREATE INDEX IF NOT EXISTS idx_hydrobio_stations_region ON hydrobio_stations(code_region);
CREATE INDEX IF NOT EXISTS idx_hydrometry_sites_region ON hydrometry_sites(code_region);
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_region ON hydrometry_stations(code_region);
CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_region ON quality_rivers_stations(code_region);
CREATE INDEX IF NOT EXISTS idx_temperature_stations_region ON temperature_stations(code_region);

-- Index sur les bassins (analyses hydrologiques)
CREATE INDEX IF NOT EXISTS idx_ecoulement_stations_bassin ON ecoulement_stations(code_bassin);
CREATE INDEX IF NOT EXISTS idx_hydrobio_stations_bassin ON hydrobio_stations(code_bassin);
CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_bassin ON quality_rivers_stations(code_bassin);
CREATE INDEX IF NOT EXISTS idx_temperature_stations_bassin ON temperature_stations(code_bassin);

-- ============================================================================
-- PARTIE 3: INDEXES SPATIAUX (GÉOMÉTRIE)
-- ============================================================================

-- Index sur latitude/longitude pour requêtes géospatiales
-- Note: Ces colonnes sont utilisées pour les requêtes de proximité et les cartes
CREATE INDEX IF NOT EXISTS idx_ecoulement_stations_latlon ON ecoulement_stations(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_hydrobio_stations_latlon ON hydrobio_stations(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_hydrometry_sites_latlon ON hydrometry_sites(latitude_site, longitude_site);
CREATE INDEX IF NOT EXISTS idx_hydrometry_stations_latlon ON hydrometry_stations(latitude_station, longitude_station);
CREATE INDEX IF NOT EXISTS idx_piezometry_stations_xy ON piezometry_stations(x, y);
CREATE INDEX IF NOT EXISTS idx_prelevements_ouvrages_latlon ON prelevements_ouvrages(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_latlon ON quality_rivers_stations(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_temperature_stations_latlon ON temperature_stations(latitude, longitude);

-- ============================================================================
-- PARTIE 4: INDEX COMPOSITES (JOINTURES FRÉQUENTES)
-- ============================================================================

-- Index composites pour filtrage multi-critères
CREATE INDEX IF NOT EXISTS idx_ecoulement_stations_dept_commune ON ecoulement_stations(code_departement, code_commune);
CREATE INDEX IF NOT EXISTS idx_hydrobio_stations_dept_commune ON hydrobio_stations(code_departement, code_commune);
CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_dept_commune ON quality_rivers_stations(code_departement, code_commune);
CREATE INDEX IF NOT EXISTS idx_temperature_stations_dept_commune ON temperature_stations(code_departement, code_commune);

-- ============================================================================
-- PARTIE 5: FOREIGN KEYS (si souhaitées)
-- ============================================================================

-- Note: Les FK ne sont PAS créées par défaut car :
-- 1. Elles ralentissent les INSERT/UPDATE
-- 2. L'intégrité référentielle est gérée côté application
-- 3. DLT peut charger les tables dans n'importe quel ordre
--
-- Décommentez si vous voulez activer les FK :

-- ALTER TABLE hydrometry_stations
--     ADD CONSTRAINT fk_hydrometry_stations_site
--     FOREIGN KEY (code_site) REFERENCES hydrometry_sites(code_site);

-- ALTER TABLE prelevements_points
--     ADD CONSTRAINT fk_prelevements_points_ouvrage
--     FOREIGN KEY (code_ouvrage) REFERENCES prelevements_ouvrages(code_ouvrage);

-- ============================================================================
-- PARTIE 6: ANALYSE ET VACUUM
-- ============================================================================

-- Mettre à jour les statistiques pour l'optimiseur de requêtes
ANALYZE ecoulement_campagnes;
ANALYZE ecoulement_stations;
ANALYZE hydrobio_stations;
ANALYZE hydrometry_sites;
ANALYZE hydrometry_stations;
ANALYZE piezometry_stations;
ANALYZE prelevements_ouvrages;
ANALYZE prelevements_points;
ANALYZE quality_groundwater_stations;
ANALYZE quality_rivers_stations;
ANALYZE temperature_stations;

-- ============================================================================
-- RÉSUMÉ
-- ============================================================================

SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    (SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'hubeau' AND tablename = pg_tables.tablename) as index_count,
    CASE
        WHEN EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = ('hubeau.' || tablename)::regclass
            AND contype = 'p'
        ) THEN '✅ YES'
        ELSE '❌ NO'
    END as has_primary_key
FROM pg_tables
WHERE schemaname = 'hubeau'
ORDER BY tablename;
