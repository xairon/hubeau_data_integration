#!/bin/bash
# Hub'Eau Pipeline - Démarrage propre avec initialisation automatique

set -e

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

echo "🚀 Hub'Eau Pipeline - Démarrage avec initialisation automatique"
echo "📊 Volume limité: 330 observations/jour pour éviter surcharge"
echo ""

# 1. Nettoyage optionnel
if [[ "$1" == "--clean" ]]; then
    log_warning "Nettoyage des volumes Docker..."
    docker-compose down -v
    docker volume prune -f
    log_success "Volumes nettoyés"
fi

# 2. Build/Pull des images
log_info "Préparation des images Docker..."
docker-compose build --no-cache dagster_webserver dagster_daemon || log_warning "Build Dagster échoué, on continue..."
docker-compose pull timescaledb postgis neo4j minio

# 3. Démarrage des bases de données avec initialisation automatique
log_info "Démarrage des bases de données..."
log_info "Les scripts d'initialisation vont s'exécuter automatiquement au premier démarrage"

# TimescaleDB avec scripts d'init
log_info "Démarrage TimescaleDB (avec scripts: ${PWD}/docker/init-scripts/timescaledb/)"
docker-compose up -d timescaledb

# PostGIS avec scripts d'init  
log_info "Démarrage PostGIS (avec scripts: ${PWD}/docker/init-scripts/postgis/)"
docker-compose up -d postgis

# Neo4j avec scripts d'init
log_info "Démarrage Neo4j (avec scripts: ${PWD}/docker/init-scripts/neo4j/)"
docker-compose up -d neo4j

# MinIO
log_info "Démarrage MinIO..."
docker-compose up -d minio

# 4. Attendre que les services soient prêts (avec healthchecks)
log_info "Attente de l'initialisation des bases de données..."

# Fonction d'attente avec healthcheck
wait_for_health() {
    local service=$1
    local max_attempts=60
    local attempt=1
    
    log_info "Attente de $service (avec healthcheck)..."
    
    while [ $attempt -le $max_attempts ]; do
        health_status=$(docker-compose ps --format json $service | jq -r '.[0].Health' 2>/dev/null || echo "")
        
        if [[ "$health_status" == "healthy" ]]; then
            log_success "$service est prêt et initialisé !"
            return 0
        elif [[ "$health_status" == "unhealthy" ]]; then
            log_warning "$service est en erreur, vérifiez les logs avec: docker-compose logs $service"
            return 1
        fi
        
        log_info "⏳ $service - Tentative $attempt/$max_attempts (statut: ${health_status:-démarrage})"
        sleep 5
        ((attempt++))
    done
    
    log_warning "$service n'est pas prêt après $((max_attempts * 5)) secondes"
    return 1
}

# Attendre les healthchecks
wait_for_health timescaledb
wait_for_health postgis  
wait_for_health neo4j

# 5. Services complémentaires
log_info "Démarrage des services complémentaires..."
docker-compose up -d redis grafana pgadmin

# 6. Dagster (après que toutes les BDD soient prêtes)
log_info "Démarrage de Dagster..."
docker-compose up -d dagster_webserver dagster_daemon

# 7. Attendre Dagster
log_info "Attente de Dagster webserver..."
attempt=1
max_attempts=30
while [ $attempt -le $max_attempts ]; do
    if curl -s http://localhost:3000/health >/dev/null 2>&1; then
        log_success "Dagster webserver est prêt !"
        break
    fi
    log_info "⏳ Dagster - Tentative $attempt/$max_attempts"
    sleep 2
    ((attempt++))
done

# 8. Vérification finale
echo ""
log_success "🎉 Hub'Eau Pipeline démarré avec succès !"
echo ""
echo "📊 VOLUMES CONFIGURÉS (limités pour tests):"
echo "   🏔️  Piézométrie      : 100 stations × 1 obs/jour   = 100 obs/jour"
echo "   🌊 Hydrométrie      : 80 stations  × 1 obs/jour   = 80 obs/jour"  
echo "   🌡️  Température      : 60 stations  × 1 obs/jour   = 60 obs/jour"
echo "   🧪 Qualité Surface  : 50 stations  × 1 analyse/jour = 50 analyses/jour"
echo "   🧪 Qualité Souterr. : 40 stations  × 1 analyse/jour = 40 analyses/jour"
echo "   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "   📈 TOTAL QUOTIDIEN  : 330 enregistrements (volume maîtrisé)"
echo ""
echo "🔗 INTERFACES DISPONIBLES:"
echo "   🎯 Dagster UI       : http://localhost:3000"
echo "   🕸️  Neo4j Browser   : http://localhost:7474 (neo4j/BrgmNeo4j2024!)"
echo "   🗄️  pgAdmin         : http://localhost:5050 (admin@brgm.fr/BrgmPgAdmin2024!)" 
echo "   📦 MinIO Console    : http://localhost:9001 (admin/BrgmMinio2024!)"
echo "   📊 Grafana          : http://localhost:3001"
echo ""
echo "🗄️ BASES DE DONNÉES INITIALISÉES:"
echo "   📊 TimescaleDB      : localhost:5432 (water_timeseries)"
echo "   🗺️  PostGIS         : localhost:5433 (water_geo)"
echo "   🕸️  Neo4j           : localhost:7687 (Sandre + SOSA)"
echo ""
echo "💡 Les schémas et données de test ont été créés automatiquement"
echo "   Prêt pour l'exécution des jobs Dagster !"
echo ""

# Affichage des logs en cas de problème
if [[ "$1" == "--logs" ]]; then
    log_info "Affichage des logs des services..."
    docker-compose logs --tail=20
fi
