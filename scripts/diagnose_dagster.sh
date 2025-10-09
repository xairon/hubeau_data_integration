#!/bin/bash
# Script de diagnostic Dagster

echo "======================================"
echo "🔍 DIAGNOSTIC DAGSTER"
echo "======================================"
echo ""

echo "1️⃣ État des conteneurs:"
docker ps -a | grep brgm

echo ""
echo "2️⃣ Logs Dagster Webserver (dernières 50 lignes):"
docker logs brgm-dagster-webserver --tail 50

echo ""
echo "3️⃣ Logs Dagster Daemon (dernières 30 lignes):"
docker logs brgm-dagster-daemon --tail 30

echo ""
echo "4️⃣ Test connexion PostgreSQL depuis Dagster:"
docker exec brgm-dagster-webserver sh -c "apk add --no-cache postgresql-client && psql postgresql://postgres:\$DAGSTER_PG_PASSWORD@dagster_postgres:5432/dagster -c 'SELECT version();'" 2>&1 || echo "❌ Connexion PostgreSQL impossible"

echo ""
echo "5️⃣ Variables d'environnement Dagster:"
docker exec brgm-dagster-webserver env | grep DAGSTER

echo ""
echo "6️⃣ Contenu du dagster.yaml:"
docker exec brgm-dagster-webserver cat /app/dagster_home/dagster.yaml

echo ""
echo "7️⃣ Test du port 8080:"
curl -I http://localhost:8080 2>&1 || echo "❌ Port 8080 non accessible"

echo ""
echo "======================================"
echo "✅ Diagnostic terminé"
echo "======================================"

