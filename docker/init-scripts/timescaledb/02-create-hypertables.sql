-- TimescaleDB Hub'Eau Pipeline - Création des hypertables
-- Exécuté après la création des tables

\c water_timeseries;

-- ==========================================
-- CRÉATION DES HYPERTABLES
-- ==========================================

-- Piézométrie
SELECT create_hypertable('piezo_observations', 'timestamp', 
                        chunk_time_interval => INTERVAL '1 day',
                        if_not_exists => TRUE);

-- Hydrométrie
SELECT create_hypertable('hydro_observations', 'timestamp',
                        chunk_time_interval => INTERVAL '1 day',
                        if_not_exists => TRUE);

-- Température
SELECT create_hypertable('temperature_observations', 'timestamp',
                        chunk_time_interval => INTERVAL '1 day',
                        if_not_exists => TRUE);

-- Qualité surface
SELECT create_hypertable('quality_surface_analyses', 'timestamp',
                        chunk_time_interval => INTERVAL '1 day',
                        if_not_exists => TRUE);

-- Qualité souterraine
SELECT create_hypertable('quality_groundwater_analyses', 'timestamp',
                        chunk_time_interval => INTERVAL '1 day',
                        if_not_exists => TRUE);

-- ==========================================
-- OPTIMISATIONS TIMESCALEDB
-- ==========================================

-- Index temporels optimisés
CREATE INDEX idx_piezo_obs_station_time ON piezo_observations (station_id, timestamp DESC);
CREATE INDEX idx_hydro_obs_station_time ON hydro_observations (station_id, timestamp DESC);
CREATE INDEX idx_temp_obs_station_time ON temperature_observations (station_id, timestamp DESC);
CREATE INDEX idx_qual_surf_analyses_station_time ON quality_surface_analyses (station_id, timestamp DESC);
CREATE INDEX idx_qual_ground_analyses_station_time ON quality_groundwater_analyses (station_id, timestamp DESC);

-- Index sur paramètres qualité
CREATE INDEX idx_qual_surf_analyses_parameter ON quality_surface_analyses (parameter_code);
CREATE INDEX idx_qual_ground_analyses_parameter ON quality_groundwater_analyses (parameter_code);

-- ==========================================
-- POLITIQUES DE COMPRESSION (après 7 jours)
-- ==========================================

ALTER TABLE piezo_observations SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'station_id'
);

ALTER TABLE hydro_observations SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'station_id'
);

ALTER TABLE temperature_observations SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'station_id'
);

ALTER TABLE quality_surface_analyses SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'station_id,parameter_code'
);

ALTER TABLE quality_groundwater_analyses SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'station_id,parameter_code'
);

-- Activation compression automatique
SELECT add_compression_policy('piezo_observations', INTERVAL '7 days');
SELECT add_compression_policy('hydro_observations', INTERVAL '7 days');
SELECT add_compression_policy('temperature_observations', INTERVAL '7 days');
SELECT add_compression_policy('quality_surface_analyses', INTERVAL '7 days');
SELECT add_compression_policy('quality_groundwater_analyses', INTERVAL '7 days');

-- ==========================================
-- VUES UTILITAIRES
-- ==========================================

-- Vue résumé quotidien toutes sources
CREATE VIEW daily_summary AS
SELECT 
    DATE(timestamp) as date,
    'piezo' as source,
    COUNT(DISTINCT station_id) as stations_count,
    COUNT(*) as observations_count
FROM piezo_observations
GROUP BY DATE(timestamp)
UNION ALL
SELECT 
    DATE(timestamp) as date,
    'hydro' as source,
    COUNT(DISTINCT station_id) as stations_count,
    COUNT(*) as observations_count
FROM hydro_observations
GROUP BY DATE(timestamp)
UNION ALL
SELECT 
    DATE(timestamp) as date,
    'temperature' as source,
    COUNT(DISTINCT station_id) as stations_count,
    COUNT(*) as observations_count
FROM temperature_observations
GROUP BY DATE(timestamp)
UNION ALL
SELECT 
    DATE(timestamp) as date,
    'quality_surface' as source,
    COUNT(DISTINCT station_id) as stations_count,
    COUNT(*) as observations_count
FROM quality_surface_analyses
GROUP BY DATE(timestamp)
UNION ALL
SELECT 
    DATE(timestamp) as date,
    'quality_groundwater' as source,
    COUNT(DISTINCT station_id) as stations_count,
    COUNT(*) as observations_count
FROM quality_groundwater_analyses
GROUP BY DATE(timestamp)
ORDER BY date DESC, source;

-- Vue stations avec dernière observation
CREATE VIEW stations_last_observation AS
SELECT 
    'piezo' as source,
    ps.station_id,
    ps.station_name,
    ps.latitude,
    ps.longitude,
    MAX(po.timestamp) as last_observation
FROM piezo_stations ps
LEFT JOIN piezo_observations po ON ps.station_id = po.station_id
WHERE ps.station_id != 'INIT_LOG'
GROUP BY ps.station_id, ps.station_name, ps.latitude, ps.longitude
UNION ALL
SELECT 
    'hydro' as source,
    hs.station_id,
    hs.station_name,
    hs.latitude,
    hs.longitude,
    MAX(ho.timestamp) as last_observation
FROM hydro_stations hs
LEFT JOIN hydro_observations ho ON hs.station_id = ho.station_id
GROUP BY hs.station_id, hs.station_name, hs.latitude, hs.longitude
UNION ALL
SELECT 
    'temperature' as source,
    ts.station_id,
    ts.station_name,
    ts.latitude,
    ts.longitude,
    MAX(to2.timestamp) as last_observation
FROM temperature_stations ts
LEFT JOIN temperature_observations to2 ON ts.station_id = to2.station_id
GROUP BY ts.station_id, ts.station_name, ts.latitude, ts.longitude
ORDER BY last_observation DESC NULLS LAST;

-- Log de finalisation
INSERT INTO piezo_observations (station_id, timestamp, water_level, measurement_method, quality_code)
VALUES ('INIT_LOG', NOW(), 0.0, 'automatic', 'init_complete');

SELECT 'TimescaleDB: Hypertables et optimisations créées avec succès' as status;
