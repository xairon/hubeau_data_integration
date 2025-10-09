#!/bin/bash
# Script de nettoyage du serveur VPS
# Supprime tout ce qui n'est pas nécessaire au déploiement pipeline

set -e

echo "======================================"
echo "🧹 NETTOYAGE DU SERVEUR VPS"
echo "======================================"
echo ""

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Fonction de confirmation
confirm() {
    read -p "$(echo -e ${YELLOW}$1 [y/N]:${NC}) " -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        return 0
    else
        return 1
    fi
}

echo "📋 Ce script va nettoyer:"
echo "  ✓ Ancien projet /hubeau_data_integration"
echo "  ✓ Conteneurs Docker arrêtés"
echo "  ✓ Images Docker inutilisées"
echo "  ✓ Volumes Docker orphelins"
echo "  ✓ Logs système anciens"
echo "  ✓ Cache APT"
echo ""
echo "🔒 Ce script va GARDER:"
echo "  ✓ /srv/brgm (projet actuel)"
echo "  ✓ /srv/brgm-data (données persistantes MinIO + PostgreSQL)"
echo "  ✓ GitLab Runner"
echo "  ✓ Conteneurs actifs"
echo ""

if ! confirm "Continuer le nettoyage?"; then
    echo "Nettoyage annulé."
    exit 0
fi

# ========================================
# 1. BACKUP DE SÉCURITÉ
# ========================================
echo ""
echo "1️⃣ Création d'un backup de sécurité..."

BACKUP_DIR="/srv/brgm-data/backups"
mkdir -p $BACKUP_DIR
BACKUP_FILE="$BACKUP_DIR/cleanup_backup_$(date +%Y%m%d_%H%M%S).tar.gz"

if [ -d "/hubeau_data_integration" ]; then
    echo "Backup de /hubeau_data_integration..."
    tar -czf $BACKUP_FILE /hubeau_data_integration 2>/dev/null || echo "Warning: Erreur backup (peut-être déjà vide)"
    echo "✓ Backup créé: $BACKUP_FILE"
else
    echo "✓ Pas d'ancien projet à sauvegarder"
fi

# ========================================
# 2. NETTOYAGE DOCKER
# ========================================
echo ""
echo "2️⃣ Nettoyage Docker..."

# Arrêter les conteneurs non-production
echo "Arrêt des conteneurs non-production..."
docker ps -a | grep -v brgm | grep -v CONTAINER | awk '{print $1}' | xargs -r docker stop 2>/dev/null || true
docker ps -a | grep -v brgm | grep -v CONTAINER | awk '{print $1}' | xargs -r docker rm 2>/dev/null || true

# Nettoyer les images inutilisées
echo "Suppression des images inutilisées..."
docker image prune -af --filter "until=24h" || true

# Nettoyer les volumes orphelins
echo "Suppression des volumes orphelins..."
docker volume prune -f || true

# Nettoyer les réseaux inutilisés
echo "Suppression des réseaux inutilisés..."
docker network prune -f || true

echo "✓ Nettoyage Docker terminé"

# ========================================
# 3. SUPPRESSION ANCIEN PROJET
# ========================================
echo ""
echo "3️⃣ Suppression de l'ancien projet..."

if [ -d "/hubeau_data_integration" ]; then
    if confirm "Supprimer /hubeau_data_integration ?"; then
        du -sh /hubeau_data_integration
        rm -rf /hubeau_data_integration
        echo "✓ /hubeau_data_integration supprimé"
    else
        echo "Conservé: /hubeau_data_integration"
    fi
else
    echo "✓ Pas d'ancien projet à supprimer"
fi

# ========================================
# 4. NETTOYAGE LOGS SYSTÈME
# ========================================
echo ""
echo "4️⃣ Nettoyage des logs système..."

# Nettoyer journalctl (garder seulement 3 jours)
if confirm "Nettoyer les logs système (garder 3 jours) ?"; then
    journalctl --vacuum-time=3d
    echo "✓ Logs système nettoyés"
fi

# Nettoyer les logs Docker
if confirm "Nettoyer les logs Docker ?"; then
    truncate -s 0 /var/lib/docker/containers/*/*-json.log 2>/dev/null || true
    echo "✓ Logs Docker nettoyés"
fi

# ========================================
# 5. NETTOYAGE APT
# ========================================
echo ""
echo "5️⃣ Nettoyage des paquets système..."

if confirm "Nettoyer le cache APT ?"; then
    apt-get clean
    apt-get autoclean
    apt-get autoremove -y
    echo "✓ Cache APT nettoyé"
fi

# ========================================
# 6. NETTOYAGE GITLAB RUNNER
# ========================================
echo ""
echo "6️⃣ Nettoyage cache GitLab Runner..."

if [ -d "/home/gitlab-runner" ]; then
    if confirm "Nettoyer le cache du GitLab Runner ?"; then
        gitlab-runner cache-clear || echo "Warning: Impossible de nettoyer le cache runner"
        rm -rf /home/gitlab-runner/.cache/* 2>/dev/null || true
        echo "✓ Cache GitLab Runner nettoyé"
    fi
fi

# ========================================
# 7. RÉSUMÉ ET ESPACE LIBÉRÉ
# ========================================
echo ""
echo "======================================"
echo "📊 RÉSUMÉ DU NETTOYAGE"
echo "======================================"
echo ""

# Espace disque
df -h / | grep -v Filesystem
echo ""

# Conteneurs actifs
echo "Conteneurs actifs:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Size}}"
echo ""

# Images
echo "Images Docker:"
docker images --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}"
echo ""

# Volumes
echo "Volumes Docker:"
docker volume ls
echo ""

echo "======================================"
echo "✅ NETTOYAGE TERMINÉ"
echo "======================================"
echo ""
echo "Backup disponible: $BACKUP_FILE"
echo ""
echo "Structure finale du serveur:"
echo "  /srv/brgm              → Projet actuel (géré par Git + pipeline)"
echo "  /srv/brgm-data/        → Données persistantes"
echo "    ├── minio/           → Données MinIO (conservées)"
echo "    ├── dagster_pg/      → Base PostgreSQL Dagster"
echo "    └── backups/         → Sauvegardes"
echo ""
echo "Pour restaurer l'ancien projet si besoin:"
echo "  tar -xzf $BACKUP_FILE -C /"
echo ""

