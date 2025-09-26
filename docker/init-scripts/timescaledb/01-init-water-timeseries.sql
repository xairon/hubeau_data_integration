-- TimescaleDB Hub'Eau Pipeline - Initialisation automatique
-- Exécuté automatiquement au premier démarrage du conteneur
-- Base: water_timeseries

-- Création de la base de données dédiée
CREATE DATABASE water_timeseries;

-- Se connecter à la nouvelle base
\c water_timeseries;

-- Activation de TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ==========================================
-- PIÉZOMÉTRIE (Volume: 100 stations, 100 obs/jour)
-- ==========================================

CREATE TABLE piezo_stations (
    station_id VARCHAR(50) PRIMARY KEY,
    station_name VARCHAR(200) NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    altitude INTEGER,
    aquifer_name VARCHAR(200),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE piezo_observations (
    station_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    water_level DOUBLE PRECISION,
    measurement_method VARCHAR(50),
    quality_code VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (station_id, timestamp)
);

-- ==========================================
-- HYDROMÉTRIE (Volume: 80 stations, 80 obs/jour)
-- ==========================================

CREATE TABLE hydro_stations (
    station_id VARCHAR(50) PRIMARY KEY,
    station_name VARCHAR(200) NOT NULL,
    water_course VARCHAR(200),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    altitude INTEGER,
    manager VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE hydro_observations (
    station_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    flow_rate DOUBLE PRECISION,
    water_height DOUBLE PRECISION,
    measurement_method VARCHAR(50),
    quality_code VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (station_id, timestamp)
);

-- ==========================================
-- TEMPÉRATURE (Volume: 60 stations, 60 obs/jour)
-- ==========================================

CREATE TABLE temperature_stations (
    station_id VARCHAR(50) PRIMARY KEY,
    station_name VARCHAR(200) NOT NULL,
    water_course VARCHAR(200),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    manager VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE temperature_observations (
    station_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    temperature DOUBLE PRECISION,
    measurement_method VARCHAR(50),
    quality_code VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (station_id, timestamp)
);

-- ==========================================
-- QUALITÉ SURFACE (Volume: 50 stations, 50 analyses/jour)
-- ==========================================

CREATE TABLE quality_surface_stations (
    station_id VARCHAR(50) PRIMARY KEY,
    station_name VARCHAR(200) NOT NULL,
    water_course VARCHAR(200),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    manager VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE quality_surface_analyses (
    station_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    parameter_code VARCHAR(50) NOT NULL,
    parameter_name VARCHAR(200),
    value DOUBLE PRECISION,
    unit VARCHAR(50),
    detection_limit DOUBLE PRECISION,
    laboratory VARCHAR(100),
    quality_code VARCHAR(10),
    validation_status VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (station_id, timestamp, parameter_code)
);

-- ==========================================
-- QUALITÉ SOUTERRAINE (Volume: 40 stations, 40 analyses/jour)
-- ==========================================

CREATE TABLE quality_groundwater_stations (
    station_id VARCHAR(50) PRIMARY KEY,
    station_name VARCHAR(200) NOT NULL,
    aquifer VARCHAR(200),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    depth INTEGER,
    manager VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE quality_groundwater_analyses (
    station_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    parameter_code VARCHAR(50) NOT NULL,
    parameter_name VARCHAR(200),
    value DOUBLE PRECISION,
    unit VARCHAR(50),
    detection_limit DOUBLE PRECISION,
    laboratory VARCHAR(100),
    quality_code VARCHAR(10),
    validation_status VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (station_id, timestamp, parameter_code)
);

-- Indices de performance
CREATE INDEX idx_piezo_stations_location ON piezo_stations (latitude, longitude);
CREATE INDEX idx_hydro_stations_location ON hydro_stations (latitude, longitude);
CREATE INDEX idx_temp_stations_location ON temperature_stations (latitude, longitude);
CREATE INDEX idx_qual_surf_stations_location ON quality_surface_stations (latitude, longitude);
CREATE INDEX idx_qual_ground_stations_location ON quality_groundwater_stations (latitude, longitude);

CREATE INDEX idx_piezo_stations_aquifer ON piezo_stations (aquifer_name);
CREATE INDEX idx_hydro_stations_course ON hydro_stations (water_course);
CREATE INDEX idx_qual_ground_stations_aquifer ON quality_groundwater_stations (aquifer);

-- Log de l'initialisation
INSERT INTO piezo_stations (station_id, station_name, latitude, longitude, altitude, aquifer_name) 
VALUES ('INIT_LOG', 'Station Log Initialisation', 0.0, 0.0, 0, 'LOG_AQUIFER');

SELECT 'TimescaleDB: Tables créées avec succès' as status;
