#!/bin/bash
# Script de gestion VPS pour Hub'Eau
# Usage: ./scripts/vps-manage.sh [command]

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

PROJECT_DIR="/srv/brgm"
DATA_DIR="/srv/brgm-data"
COMPOSE_FILE="docker-compose.production.yml"

log_info() { echo -e "${GREEN}✓${NC} $1"; }
log_warn() { echo -e "${YELLOW}⚠${NC} $1"; }
log_error() { echo -e "${RED}✗${NC} $1"; }
log_cmd() { echo -e "${BLUE}▶${NC} $1"; }

show_help() {
    cat << EOF
🛠️  Hub'Eau VPS Management Tool

Usage: $0 [command]

Commands:
    status          Afficher l'état des services
    logs            Voir les logs (Ctrl+C pour quitter)
    logs [service]  Voir les logs d'un service spécifique
    restart         Redémarrer tous les services
    restart [svc]   Redémarrer un service spécifique
    stop            Arrêter tous les services
    start           Démarrer tous les services
    deploy          Déployer la dernière version
    backup          Créer une sauvegarde manuelle
    restore [file]  Restaurer depuis une sauvegarde
    stats           Voir l'utilisation des ressources
    storage         Voir l'utilisation du stockage MinIO
    clean           Nettoyer les anciennes images Docker
    update          Mettre à jour le projet depuis Git
    health          Vérifier la santé des services

Examples:
    $0 status                    # État des services
    $0 logs dagster_daemon       # Logs du daemon Dagster
    $0 restart minio             # Redémarrer MinIO
    $0 backup                    # Créer une sauvegarde
    $0 restore backup_file.tar.gz  # Restaurer

EOF
}

check_requirements() {
    if [ ! -d "$PROJECT_DIR" ]; then
        log_error "Projet non trouvé dans $PROJECT_DIR"
        exit 1
    fi
    cd $PROJECT_DIR
}

cmd_status() {
    log_info "État des services:"
    docker compose -f $COMPOSE_FILE ps
}

cmd_logs() {
    if [ -z "$1" ]; then
        log_info "Logs de tous les services (Ctrl+C pour quitter):"
        docker compose -f $COMPOSE_FILE logs -f
    else
        log_info "Logs de $1 (Ctrl+C pour quitter):"
        docker compose -f $COMPOSE_FILE logs -f $1
    fi
}

cmd_restart() {
    if [ -z "$1" ]; then
        log_info "Redémarrage de tous les services..."
        docker compose -f $COMPOSE_FILE restart
        log_info "Services redémarrés"
    else
        log_info "Redémarrage de $1..."
        docker compose -f $COMPOSE_FILE restart $1
        log_info "$1 redémarré"
    fi
}

cmd_stop() {
    log_info "Arrêt de tous les services..."
    docker compose -f $COMPOSE_FILE down
    log_info "Services arrêtés"
}

cmd_start() {
    log_info "Démarrage de tous les services..."
    docker compose -f $COMPOSE_FILE up -d
    log_info "Services démarrés"
    cmd_health
}

cmd_deploy() {
    log_info "Déploiement de la dernière version..."
    bash scripts/deploy.sh
}

cmd_backup() {
    BACKUP_NAME="manual_backup_$(date +%Y%m%d_%H%M%S).tar.gz"
    log_info "Création sauvegarde: $BACKUP_NAME"

    mkdir -p $DATA_DIR/backups
    cd $DATA_DIR
    tar -czf backups/$BACKUP_NAME minio/

    BACKUP_SIZE=$(du -h backups/$BACKUP_NAME | cut -f1)
    log_info "Sauvegarde créée: $BACKUP_SIZE"
    log_info "Fichier: $DATA_DIR/backups/$BACKUP_NAME"
}

cmd_restore() {
    if [ -z "$1" ]; then
        log_error "Usage: $0 restore [backup_file.tar.gz]"
        log_info "Sauvegardes disponibles:"
        ls -lh $DATA_DIR/backups/*.tar.gz 2>/dev/null || log_warn "Aucune sauvegarde trouvée"
        exit 1
    fi

    BACKUP_FILE="$DATA_DIR/backups/$1"
    if [ ! -f "$BACKUP_FILE" ]; then
        log_error "Sauvegarde non trouvée: $BACKUP_FILE"
        exit 1
    fi

    log_warn "ATTENTION: Cette opération va remplacer les données actuelles"
    read -p "Continuer? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Annulé"
        exit 0
    fi

    log_info "Arrêt des services..."
    docker compose -f $COMPOSE_FILE down

    log_info "Restauration depuis $1..."
    cd $DATA_DIR
    rm -rf minio.old 2>/dev/null || true
    mv minio minio.old 2>/dev/null || true
    tar -xzf $BACKUP_FILE

    log_info "Redémarrage des services..."
    cd $PROJECT_DIR
    docker compose -f $COMPOSE_FILE up -d

    log_info "Restauration terminée"
}

cmd_stats() {
    log_info "Utilisation des ressources:"
    docker stats --no-stream
}

cmd_storage() {
    log_info "Utilisation du stockage MinIO:"
    echo ""
    du -sh $DATA_DIR/minio/* 2>/dev/null || log_warn "Aucune donnée MinIO"
    echo ""
    log_info "Total MinIO:"
    du -sh $DATA_DIR/minio/
    echo ""
    log_info "Espace disque disponible:"
    df -h $DATA_DIR
}

cmd_clean() {
    log_info "Nettoyage des anciennes images Docker..."
    docker image prune -a -f
    log_info "Nettoyage des volumes non utilisés..."
    docker volume prune -f
    log_info "Nettoyage des réseaux non utilisés..."
    docker network prune -f
    log_info "Nettoyage terminé"
}

cmd_update() {
    log_info "Mise à jour depuis Git..."
    git fetch origin
    git status
    echo ""
    log_warn "Pour appliquer les changements, lancez: $0 deploy"
}

cmd_health() {
    log_info "Vérification de la santé des services..."
    echo ""

    # MinIO
    if docker exec brgm-minio curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; then
        log_info "MinIO: Opérationnel"
    else
        log_error "MinIO: Problème détecté"
    fi

    # Dagster
    if curl -sf http://localhost:8080/server_info > /dev/null 2>&1; then
        log_info "Dagster: Opérationnel"
    else
        log_error "Dagster: Problème détecté"
    fi

    # PostgreSQL
    if docker exec brgm-dagster-postgres pg_isready -U postgres > /dev/null 2>&1; then
        log_info "PostgreSQL: Opérationnel"
    else
        log_error "PostgreSQL: Problème détecté"
    fi

    echo ""
    log_info "URLs:"
    echo "  Dagster UI:    http://srv991054.hstgr.cloud:8080"
    echo "  MinIO Console: http://srv991054.hstgr.cloud:9001"
}

# Main
case "${1:-help}" in
    status)     check_requirements && cmd_status ;;
    logs)       check_requirements && cmd_logs "$2" ;;
    restart)    check_requirements && cmd_restart "$2" ;;
    stop)       check_requirements && cmd_stop ;;
    start)      check_requirements && cmd_start ;;
    deploy)     check_requirements && cmd_deploy ;;
    backup)     check_requirements && cmd_backup ;;
    restore)    check_requirements && cmd_restore "$2" ;;
    stats)      check_requirements && cmd_stats ;;
    storage)    check_requirements && cmd_storage ;;
    clean)      check_requirements && cmd_clean ;;
    update)     check_requirements && cmd_update ;;
    health)     check_requirements && cmd_health ;;
    help|--help|-h) show_help ;;
    *)          log_error "Commande inconnue: $1" && echo "" && show_help && exit 1 ;;
esac
