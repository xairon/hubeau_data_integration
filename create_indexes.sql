-- Indexation Optimisée pour Pipeline Hub'Eau
-- Objectif : Accélérer les jointures (ERA5) et les filtres (Piezo)

-- 1. ERA5 Time Series (Critical for Joins)
-- Index composite sur (lat, lon, time) pour la jointure spatio-temporelle rapide
CREATE INDEX IF NOT EXISTS idx_era5_lat_lon_time 
ON staging.era5_france_timeseries (latitude, longitude, time);

-- Index individuels utiles pour d'autres types de requêtes
CREATE INDEX IF NOT EXISTS idx_era5_time 
ON staging.era5_france_timeseries (time);

-- 2. Piezometry Chroniques (Filtrage & Join)
CREATE INDEX IF NOT EXISTS idx_piezo_chroniques_full
ON staging.piezometry_chroniques_raw (code_bss, date_mesure);

-- 3. Piezometry Stations (Spatial Join)
CREATE INDEX IF NOT EXISTS idx_piezo_stations_coords
ON staging.piezometry_stations_raw (x, y);

CREATE INDEX IF NOT EXISTS idx_piezo_stations_code_bss
ON staging.piezometry_stations_raw (code_bss);
