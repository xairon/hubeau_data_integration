#!/bin/bash
# Script pour réinitialiser le schéma PostgreSQL Hub'Eau
# Utile quand la base existe déjà et qu'on veut appliquer le nouveau schéma

set -e

echo "⚠️  ATTENTION: Ce script va supprimer et recréer le schéma hubeau!"
echo "Toutes les données existantes dans ce schéma seront perdues."
echo ""
read -p "Êtes-vous sûr? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Annulé."
    exit 1
fi

echo ""
echo "🔍 Détection de l'environnement..."

# Détection de l'environnement
if [ -f /.dockerenv ]; then
    # Dans Docker
    PSQL="psql -U postgres -d postgres"
    echo "📍 Mode: Conteneur Docker"
else
    # Local ou via docker exec
    if docker ps | grep -q "postgres\|brgm-postgres"; then
        PSQL="docker exec -i postgres psql -U postgres -d postgres 2>/dev/null || docker exec -i brgm-postgres psql -U postgres -d postgres"
        echo "📍 Mode: Docker exec (local)"
    else
        echo "❌ Aucun conteneur PostgreSQL trouvé"
        echo "Veuillez démarrer le conteneur PostgreSQL avant d'exécuter ce script."
        exit 1
    fi
fi

echo ""
echo "🗑️  Suppression du schéma existant..."
$PSQL -c "DROP SCHEMA IF EXISTS hubeau CASCADE;" || {
    echo "❌ Erreur lors de la suppression du schéma"
    exit 1
}

echo "📦 Création du nouveau schéma..."
$PSQL < docker/init-scripts/postgres/01_create_schema.sql || {
    echo "❌ Erreur lors de la création du schéma"
    exit 1
}

echo ""
echo "✅ Schéma 'hubeau' réinitialisé avec succès!"
echo "📋 Prochaines étapes:"
echo "   1. Relancer vos jobs Dagster"
echo "   2. DLT utilisera maintenant les tables pré-définies"
echo ""
