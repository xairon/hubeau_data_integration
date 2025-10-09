#!/bin/bash
set -e

echo "======================================"
echo "🔧 CORRECTION CONNEXION DAGSTER → PostgreSQL"
echo "======================================"
echo ""

# Vérifier qu'on est dans le bon répertoire
if [ ! -f "docker-compose.production.yml" ]; then
    echo "❌ Erreur: Exécutez ce script depuis /srv/brgm"
    exit 1
fi

echo "🛑 Arrêt des conteneurs Dagster..."
docker compose -f docker-compose.production.yml stop dagster_webserver dagster_daemon

echo ""
echo "🗑️  Suppression des conteneurs Dagster..."
docker compose -f docker-compose.production.yml rm -f dagster_webserver dagster_daemon

echo ""
echo "✅ Configuration dagster.yaml mise à jour (utilise les variables d'environnement)"

echo ""
echo "🚀 Redémarrage des conteneurs Dagster..."
docker compose -f docker-compose.production.yml up -d dagster_webserver dagster_daemon

echo ""
echo "⏳ Attente démarrage Dagster (20 secondes)..."
sleep 20

echo ""
echo "📋 Vérification des logs Dagster webserver:"
docker logs brgm-dagster-webserver --tail 50

echo ""
echo "======================================"
echo "✅ CORRECTION TERMINÉE"
echo "======================================"
echo ""
echo "🌐 Accédez à Dagster: http://srv991054.hstgr.cloud:8080"
echo ""
echo "Pour vérifier les logs complets:"
echo "  docker logs -f brgm-dagster-webserver"
echo "  docker logs -f brgm-dagster-daemon"

