#!/bin/bash
set -e

# Script de déploiement automatique pour VPS Hostinger
# Usage: ./scripts/deploy.sh

echo "🚀 Démarrage du déploiement..."

# Couleurs pour les logs
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

PROJECT_DIR="/srv/brgm"
DATA_DIR="/srv/brgm-data"

# Fonction de log
log_info() {
    echo -e "${GREEN}✓${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

log_error() {
    echo -e "${RED}✗${NC} $1"
}

# 1. Créer les répertoires de données persistantes s'ils n'existent pas
log_info "Vérification des répertoires de données..."
mkdir -p $DATA_DIR/minio
mkdir -p $DATA_DIR/dagster_pg
mkdir -p $DATA_DIR/backups

# 2. Sauvegarder l'état actuel de MinIO (si existe)
if [ -d "$DATA_DIR/minio/bronze" ]; then
    BACKUP_NAME="minio_backup_$(date +%Y%m%d_%H%M%S).tar.gz"
    log_info "Sauvegarde MinIO existant: $BACKUP_NAME"
    tar -czf "$DATA_DIR/backups/$BACKUP_NAME" -C "$DATA_DIR" minio 2>/dev/null || log_warn "Pas de données MinIO à sauvegarder"

    # Garder seulement les 5 dernières sauvegardes
    cd "$DATA_DIR/backups" && ls -t minio_backup_*.tar.gz 2>/dev/null | tail -n +6 | xargs -r rm
fi

# 3. Arrêter les conteneurs existants (sans supprimer les données)
log_info "Arrêt des conteneurs existants..."
cd $PROJECT_DIR
docker-compose -f docker-compose.production.yml down --remove-orphans || log_warn "Aucun conteneur à arrêter"

# 4. Supprimer les anciennes images pour forcer le rebuild
log_info "Nettoyage des anciennes images..."
docker rmi hubeau-dagster:latest 2>/dev/null || log_warn "Pas d'image à supprimer"

# 5. Build de la nouvelle image
log_info "Build de la nouvelle image..."
docker-compose -f docker-compose.production.yml build --no-cache

# 6. Démarrer les services
log_info "Démarrage des services..."
docker-compose -f docker-compose.production.yml up -d

# 7. Attendre que les services soient prêts
log_info "Attente de la disponibilité des services..."
TIMEOUT=120
ELAPSED=0

# Attendre MinIO
until docker exec brgm-minio curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; do
    if [ $ELAPSED -ge $TIMEOUT ]; then
        log_error "Timeout: MinIO n'a pas démarré"
        exit 1
    fi
    sleep 2
    ELAPSED=$((ELAPSED + 2))
done
log_info "MinIO opérationnel"

# Attendre Dagster
ELAPSED=0
until curl -sf http://localhost:8080/server_info > /dev/null 2>&1; do
    if [ $ELAPSED -ge $TIMEOUT ]; then
        log_error "Timeout: Dagster n'a pas démarré"
        exit 1
    fi
    sleep 2
    ELAPSED=$((ELAPSED + 2))
done
log_info "Dagster opérationnel"

# 8. Vérifier l'état des conteneurs
log_info "État des conteneurs:"
docker-compose -f docker-compose.production.yml ps

# 9. Afficher les logs des dernières 20 lignes
log_info "Derniers logs:"
docker-compose -f docker-compose.production.yml logs --tail=20

# 10. Afficher les informations de connexion
echo ""
log_info "==================== DÉPLOIEMENT TERMINÉ ===================="
echo ""
echo "📊 Dagster UI:    http://srv991054.hstgr.cloud:8080"
echo "🗄️  MinIO Console: http://srv991054.hstgr.cloud:9001"
echo ""
echo "Credentials MinIO:"
echo "  User: $MINIO_USER"
echo "  Pass: $MINIO_PASS"
echo ""
log_info "Les données MinIO sont persistées dans: $DATA_DIR/minio"
log_info "Sauvegardes disponibles dans: $DATA_DIR/backups"
echo ""
echo "Pour voir les logs:"
echo "  docker-compose -f docker-compose.production.yml logs -f"
echo ""
log_info "============================================================="
