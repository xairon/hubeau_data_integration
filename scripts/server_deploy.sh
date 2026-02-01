#!/bin/bash
# ============================================================================
# Script de déploiement serveur - Nettoyage complet et redéploiement
# À exécuter sur le serveur: bash scripts/server_deploy.sh
# ============================================================================

set -e  # Exit on error

echo "🚀 Hub'Eau Pipeline - Déploiement serveur"
echo "=========================================="
echo ""

# Vérifier qu'on est dans le bon dossier
if [ ! -f "docker-compose.yml" ]; then
    echo "❌ Erreur: docker-compose.yml non trouvé"
    echo "Exécutez ce script depuis le dossier ~/hubeau_data_integration"
    exit 1
fi

echo "📂 Dossier courant: $(pwd)"
echo ""

# ÉTAPE 1: Arrêter tous les conteneurs
echo "⏸️  ÉTAPE 1/7: Arrêt des conteneurs..."
docker compose down 2>/dev/null || echo "Aucun conteneur à arrêter"
echo "✅ Conteneurs arrêtés"
echo ""

# ÉTAPE 2: Supprimer tous les conteneurs (même stopped)
echo "🗑️  ÉTAPE 2/7: Suppression de tous les conteneurs Hub'Eau..."
docker ps -a --filter "name=brgm-" --format "{{.Names}}" | xargs -r docker rm -f 2>/dev/null || true
echo "✅ Conteneurs supprimés"
echo ""

# ÉTAPE 3: Supprimer toutes les images
echo "🗑️  ÉTAPE 3/7: Suppression des images Docker..."
docker images --filter "reference=hubeau-*" --format "{{.Repository}}:{{.Tag}}" | xargs -r docker rmi -f 2>/dev/null || true
echo "✅ Images supprimées"
echo ""

# ÉTAPE 4: Supprimer les anciens volumes (ATTENTION: PERTE DE DONNÉES)
echo "⚠️  ÉTAPE 4/7: Suppression des anciens volumes..."
echo "ATTENTION: Ceci va supprimer toutes les données existantes!"
read -p "Continuer? (y/N) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker volume rm postgres_data dagster_pg_data cloudbeaver_data 2>/dev/null || echo "Volumes anciens déjà supprimés ou inexistants"
    echo "✅ Anciens volumes supprimés"
else
    echo "❌ Annulé par l'utilisateur"
    exit 1
fi
echo ""

# ÉTAPE 5: Git pull
echo "📥 ÉTAPE 5/7: Mise à jour du code (git pull)..."
git pull origin main
echo "✅ Code mis à jour"
echo ""

# ÉTAPE 6: Créer les nouveaux volumes EXTERNES
echo "📦 ÉTAPE 6/7: Création des volumes externes..."
bash scripts/init_volumes.sh
echo "✅ Volumes externes créés"
echo ""

# ÉTAPE 7: Build et démarrage
echo "🏗️  ÉTAPE 7/7: Build et démarrage des services..."
docker compose build --no-cache
docker compose up -d
echo "✅ Services démarrés"
echo ""

# Attendre que les services soient healthy
echo "⏳ Attente du démarrage des services (60s)..."
sleep 60

# Vérifier le statut
echo ""
echo "📊 STATUT DES SERVICES:"
echo "======================"
docker compose ps
echo ""

# Vérifier les volumes
echo "📦 VOLUMES CRÉÉS:"
echo "================"
docker volume ls | grep brgm_
echo ""

echo "✅ DÉPLOIEMENT TERMINÉ!"
echo ""
echo "🌐 Services disponibles:"
echo "  - Dagster UI: http://$(hostname -I | awk '{print $1}'):49500"
echo "  - Adminer:    http://$(hostname -I | awk '{print $1}'):49501"
echo "  - PostgreSQL: $(hostname -I | awk '{print $1}'):49502"
echo ""
echo "📋 Prochaines étapes:"
echo "  1. Vérifier les logs: docker compose logs -f dlt_worker"
echo "  2. Accéder à Dagster UI et lancer full_bootstrap_job"
echo ""
