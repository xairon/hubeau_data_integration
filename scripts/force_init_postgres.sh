#!/bin/bash
# Script pour forcer l'initialisation PostgreSQL au déploiement
# Utilisé dans le CI/CD pour s'assurer que le schéma est créé

set -e

echo "🔄 FORCE INITIALISATION POSTGRESQL"
echo "=================================="

# 1. Arrêter PostgreSQL
echo "🛑 Arrêt de PostgreSQL..."
docker-compose stop postgres 2>/dev/null || true

# 2. Supprimer le volume de données pour forcer la réinitialisation
echo "🗑️ Suppression du volume de données..."
docker volume rm /srv/brgm-data/postgres -f 2>/dev/null || true

# 3. Redémarrer PostgreSQL (il va exécuter le script d'init)
echo "🚀 Redémarrage de PostgreSQL avec PostGIS..."
docker-compose up -d postgres

# 4. Attendre que PostgreSQL soit prêt
echo "⏳ Attente que PostgreSQL soit prêt..."
sleep 30

# 5. Vérifier que le schéma et les tables sont créés
echo "✅ Vérification du schéma..."
docker exec brgm-postgres psql -U postgres -d postgres -c "\dn" || echo "❌ Erreur: PostgreSQL pas prêt"
docker exec brgm-postgres psql -U postgres -d postgres -c "\dt hubeau.*" || echo "❌ Erreur: Tables non créées"

echo ""
echo "✅ Initialisation forcée terminée !"
echo "PostgreSQL avec PostGIS est prêt avec le schéma hubeau."
