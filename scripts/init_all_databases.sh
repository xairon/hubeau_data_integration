#!/bin/bash
# Hub'Eau Pipeline - Initialisation complète des bases de données
# Volumes limités pour éviter surcharge

set -e

echo "🚀 Initialisation Hub'Eau Pipeline - Bases de données"
echo "Volume limité: 1 observation/jour/station pour tests"

# Couleurs pour les logs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Attendre que les services soient prêts
wait_for_service() {
    local service=$1
    local port=$2
    local max_attempts=30
    local attempt=1
    
    log_info "Attente du service $service sur le port $port..."
    
    while [ $attempt -le $max_attempts ]; do
        if docker-compose exec -T $service nc -z localhost $port 2>/dev/null; then
            log_success "$service est prêt !"
            return 0
        fi
        
        log_warning "Tentative $attempt/$max_attempts - $service pas encore prêt..."
        sleep 2
        ((attempt++))
    done
    
    log_error "$service n'est pas disponible après $max_attempts tentatives"
    return 1
}

# 1. VÉRIFICATION DES SERVICES
log_info "Vérification des services Docker..."

# Démarrer les services s'ils ne sont pas actifs
docker-compose up -d timescaledb postgis neo4j minio

# Attendre que les services soient prêts
wait_for_service timescaledb 5432
wait_for_service postgis 5432
wait_for_service neo4j 7687
wait_for_service minio 9000

# 2. TIMESCALEDB - Séries temporelles
log_info "Initialisation TimescaleDB..."

docker-compose exec -T timescaledb psql -U postgres -d postgres << 'EOF'
-- Création de la base water_timeseries
CREATE DATABASE water_timeseries;
\c water_timeseries;
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- PIÉZOMÉTRIE (Volume: 100 stations, 100 obs/jour)
CREATE TABLE IF NOT EXISTS piezo_stations (
    station_id VARCHAR(50) PRIMARY KEY,
    station_name VARCHAR(200),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    altitude INTEGER,
    aquifer_name VARCHAR(200),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS piezo_observations (
    station_id VARCHAR(50),
    timestamp TIMESTAMP NOT NULL,
    water_level DOUBLE PRECISION,
    measurement_method VARCHAR(50),
    quality_code VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (station_id, timestamp)
);

-- HYDROMÉTRIE (Volume: 80 stations, 80 obs/jour)
CREATE TABLE IF NOT EXISTS hydro_stations (
    station_id VARCHAR(50) PRIMARY KEY,
    station_name VARCHAR(200),
    water_course VARCHAR(200),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    altitude INTEGER,
    manager VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hydro_observations (
    station_id VARCHAR(50),
    timestamp TIMESTAMP NOT NULL,
    flow_rate DOUBLE PRECISION,
    water_height DOUBLE PRECISION,
    measurement_method VARCHAR(50),
    quality_code VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (station_id, timestamp)
);

-- TEMPÉRATURE (Volume: 60 stations, 60 obs/jour)
CREATE TABLE IF NOT EXISTS temperature_stations (
    station_id VARCHAR(50) PRIMARY KEY,
    station_name VARCHAR(200),
    water_course VARCHAR(200),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    manager VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS temperature_observations (
    station_id VARCHAR(50),
    timestamp TIMESTAMP NOT NULL,
    temperature DOUBLE PRECISION,
    measurement_method VARCHAR(50),
    quality_code VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (station_id, timestamp)
);

-- QUALITÉ SURFACE (Volume: 50 stations, 50 analyses/jour)
CREATE TABLE IF NOT EXISTS quality_surface_stations (
    station_id VARCHAR(50) PRIMARY KEY,
    station_name VARCHAR(200),
    water_course VARCHAR(200),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    manager VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS quality_surface_analyses (
    station_id VARCHAR(50),
    timestamp TIMESTAMP NOT NULL,
    parameter_code VARCHAR(50),
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

-- QUALITÉ SOUTERRAINE (Volume: 40 stations, 40 analyses/jour)
CREATE TABLE IF NOT EXISTS quality_groundwater_stations (
    station_id VARCHAR(50) PRIMARY KEY,
    station_name VARCHAR(200),
    aquifer VARCHAR(200),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    depth INTEGER,
    manager VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS quality_groundwater_analyses (
    station_id VARCHAR(50),
    timestamp TIMESTAMP NOT NULL,
    parameter_code VARCHAR(50),
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

SELECT 'TimescaleDB: Tables créées' as status;
EOF

# Création des hypertables (commande séparée car parfois problématique)
docker-compose exec -T timescaledb psql -U postgres -d water_timeseries << 'EOF'
SELECT create_hypertable('piezo_observations', 'timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT create_hypertable('hydro_observations', 'timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT create_hypertable('temperature_observations', 'timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT create_hypertable('quality_surface_analyses', 'timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT create_hypertable('quality_groundwater_analyses', 'timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

SELECT 'TimescaleDB: Hypertables créées' as status;
EOF

log_success "TimescaleDB initialisé - 330 observations/jour max"

# 3. POSTGIS - Données géographiques
log_info "Initialisation PostGIS..."

docker-compose exec -T postgis psql -U postgres -d postgres << 'EOF'
CREATE DATABASE water_geo;
\c water_geo;
CREATE EXTENSION IF NOT EXISTS postgis;

-- BDLISA - Masses d'eau souterraine
CREATE TABLE IF NOT EXISTS bdlisa_masses_eau_souterraine (
    id VARCHAR(50) PRIMARY KEY,
    nom VARCHAR(200) NOT NULL,
    type_masse_eau VARCHAR(100),
    bassin_district VARCHAR(100),
    geom GEOMETRY(MULTIPOLYGON, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);

-- BDLISA - Formations géologiques
CREATE TABLE IF NOT EXISTS bdlisa_formations_geologiques (
    id VARCHAR(50) PRIMARY KEY,
    formation_name VARCHAR(200) NOT NULL,
    age_geologique VARCHAR(100),
    lithologie VARCHAR(200),
    geom GEOMETRY(MULTIPOLYGON, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Index spatiaux
CREATE INDEX IF NOT EXISTS idx_masses_eau_geom ON bdlisa_masses_eau_souterraine USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_formations_geom ON bdlisa_formations_geologiques USING GIST(geom);

SELECT 'PostGIS: Schéma géographique créé' as status;
EOF

log_success "PostGIS initialisé - Données géographiques prêtes"

# 4. NEO4J - Thésaurus et ontologie
log_info "Initialisation Neo4j..."

# Attendre que Neo4j soit complètement prêt
sleep 10

docker-compose exec -T neo4j cypher-shell -u neo4j -p BrgmNeo4j2024! << 'EOF'
// Contraintes Sandre
CREATE CONSTRAINT sandre_param_code IF NOT EXISTS FOR (p:SandreParametres) REQUIRE p.code IS UNIQUE;
CREATE CONSTRAINT sandre_unite_code IF NOT EXISTS FOR (u:SandreUnites) REQUIRE u.code IS UNIQUE;
CREATE CONSTRAINT sandre_methode_code IF NOT EXISTS FOR (m:SandreMethodes) REQUIRE m.code IS UNIQUE;

// Contraintes SOSA
CREATE CONSTRAINT sosa_platform_id IF NOT EXISTS FOR (p:Platform) REQUIRE p.station_id IS UNIQUE;
CREATE CONSTRAINT sosa_sensor_id IF NOT EXISTS FOR (s:Sensor) REQUIRE s.sensor_id IS UNIQUE;

// Paramètres Sandre exemples
CREATE (p1:SandreParametres {code: "1340", libelle: "Nitrates", unite_defaut: "mg/L", domaine: "Chimie"});
CREATE (p2:SandreParametres {code: "1335", libelle: "Atrazine", unite_defaut: "µg/L", domaine: "Pesticides"});
CREATE (p3:SandreParametres {code: "1301", libelle: "Température", unite_defaut: "°C", domaine: "Physique"});

// Unités Sandre
CREATE (u1:SandreUnites {code: "133", libelle: "mg/L", symbole: "mg/L"});
CREATE (u2:SandreUnites {code: "146", libelle: "µg/L", symbole: "µg/L"});
CREATE (u3:SandreUnites {code: "27", libelle: "°C", symbole: "°C"});

// Relations
CREATE (p1)-[:MESURE_AVEC_UNITE]->(u1);
CREATE (p2)-[:MESURE_AVEC_UNITE]->(u2);
CREATE (p3)-[:MESURE_AVEC_UNITE]->(u3);

RETURN "Neo4j: Thésaurus Sandre initialisé" as status;
EOF

log_success "Neo4j initialisé - Thésaurus Sandre + Structure SOSA"

# 5. MINIO - Stockage objet
log_info "Initialisation MinIO..."

# Création des buckets via API MinIO
docker-compose exec -T minio mc alias set local http://localhost:9000 admin BrgmMinio2024! || true
docker-compose exec -T minio mc mb local/hubeau-bronze || true
docker-compose exec -T minio mc mb local/hubeau-silver || true
docker-compose exec -T minio mc mb local/hubeau-gold || true

log_success "MinIO initialisé - Buckets créés"

# 6. RÉSUMÉ ET VALIDATION
log_info "Validation de l'initialisation..."

echo ""
echo "📊 RÉSUMÉ - VOLUMES LIMITÉS POUR TESTS:"
echo "🏔️  Piézométrie     : 100 stations × 1 obs/jour   = 100 obs/jour"
echo "🌊 Hydrométrie     : 80 stations  × 1 obs/jour   = 80 obs/jour"
echo "🌡️  Température     : 60 stations  × 1 obs/jour   = 60 obs/jour"
echo "🧪 Qualité Surface : 50 stations  × 1 analyse/jour = 50 analyses/jour"
echo "🧪 Qualité Souterr.: 40 stations  × 1 analyse/jour = 40 analyses/jour"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📈 TOTAL QUOTIDIEN : 330 enregistrements (volume maîtrisé)"
echo ""

# Test de connectivité
log_info "Test de connectivité..."

# TimescaleDB
TIMESCALE_COUNT=$(docker-compose exec -T timescaledb psql -U postgres -d water_timeseries -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public';" | tr -d ' ')
log_success "TimescaleDB: $TIMESCALE_COUNT tables créées"

# PostGIS
POSTGIS_COUNT=$(docker-compose exec -T postgis psql -U postgres -d water_geo -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public';" | tr -d ' ')
log_success "PostGIS: $POSTGIS_COUNT tables créées"

# Neo4j
NEO4J_COUNT=$(docker-compose exec -T neo4j cypher-shell -u neo4j -p BrgmNeo4j2024! "MATCH (n:SandreParametres) RETURN count(n) as count;" | grep -o '[0-9]*' | head -1)
log_success "Neo4j: $NEO4J_COUNT paramètres Sandre créés"

echo ""
log_success "🎉 Initialisation complète terminée !"
echo ""
echo "🔗 URLs d'accès:"
echo "   - Dagster UI: http://localhost:3000"
echo "   - Neo4j Browser: http://localhost:7474"
echo "   - pgAdmin: http://localhost:5050"
echo "   - MinIO Console: http://localhost:9001"
echo ""
echo "💡 Volume quotidien limité à 330 enregistrements pour éviter surcharge"
echo "   Parfait pour les tests et développement !"
echo ""
