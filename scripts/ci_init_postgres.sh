#!/bin/bash
# Script pour le CI/CD - Force l'initialisation PostgreSQL
# Ce script est appelé dans le pipeline GitLab

set -e

echo "🚀 CI/CD: INITIALISATION POSTGRESQL"
echo "==================================="

# 1. Aller dans le répertoire du projet
cd /srv/brgm

# 2. Arrêter tous les services
echo "🛑 Arrêt des services..."
docker-compose down

# 3. Supprimer le volume PostgreSQL pour forcer la réinitialisation
echo "🗑️ Suppression du volume PostgreSQL..."
docker volume rm /srv/brgm-data/postgres -f 2>/dev/null || true

# 4. Redémarrer PostgreSQL (il va exécuter le script d'init avec PostGIS)
echo "🚀 Redémarrage de PostgreSQL avec PostGIS..."
docker-compose up -d postgres

# 5. Attendre que PostgreSQL soit prêt
echo "⏳ Attente que PostgreSQL soit prêt..."
sleep 30

# 6. Vérifier que le schéma est créé
echo "✅ Vérification du schéma..."
docker exec brgm-postgres psql -U postgres -d postgres -c "\dn"
docker exec brgm-postgres psql -U postgres -d postgres -c "\dt hubeau.*"

# 7. Redémarrer tous les services
echo "🚀 Redémarrage de tous les services..."
docker-compose up -d

echo ""
echo "✅ CI/CD: Initialisation PostgreSQL terminée !"
echo "PostgreSQL avec PostGIS est prêt avec le schéma hubeau."
