#!/bin/bash
# ============================================================================
# Script d'initialisation des volumes Docker externes
# DOIT être exécuté AVANT le premier "docker compose up"
# ============================================================================
# Protection maximale contre suppression accidentelle des données
# Les volumes externes ne sont PAS supprimés par "docker compose down -v"
# ============================================================================

set -e  # Exit on error

echo "🔧 Initialisation des volumes Docker externes pour Hub'Eau Pipeline..."
echo ""

# Couleurs pour output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Fonction pour créer un volume s'il n'existe pas
create_volume_if_not_exists() {
    local volume_name=$1
    local description=$2

    if docker volume inspect "$volume_name" >/dev/null 2>&1; then
        echo -e "${YELLOW}⚠️  Volume $volume_name existe déjà (aucune action)${NC}"
    else
        echo "📦 Création du volume: $volume_name ($description)..."
        docker volume create "$volume_name"
        echo -e "${GREEN}✅ Volume $volume_name créé${NC}"
    fi
}

# Créer les volumes externes
echo "📦 Création des volumes persistants..."
echo ""

create_volume_if_not_exists "brgm_postgres_data" "Base de données PostgreSQL/TimescaleDB"
create_volume_if_not_exists "brgm_dagster_pg_data" "Métadonnées Dagster"
create_volume_if_not_exists "brgm_cloudbeaver_data" "Configuration CloudBeaver"

echo ""
echo -e "${GREEN}✅ Tous les volumes sont prêts!${NC}"
echo ""
echo "📋 Volumes créés:"
docker volume ls | grep "brgm_"
echo ""
echo -e "${GREEN}🚀 Vous pouvez maintenant lancer: docker compose up -d${NC}"
echo ""
echo "⚠️  IMPORTANT - Protection des données:"
echo "   - Les volumes NE SERONT PAS supprimés par 'docker compose down -v'"
echo "   - Pour supprimer les volumes: docker volume rm brgm_postgres_data brgm_dagster_pg_data brgm_cloudbeaver_data"
echo "   - Backup recommandé: docker exec brgm-postgres pg_dump -U postgres postgres > backup.sql"
echo ""
