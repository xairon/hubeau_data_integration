#!/bin/bash
# Check rapide de l'état de Dagster

echo "🔍 CHECK RAPIDE DAGSTER"
echo ""

echo "1. État conteneurs:"
docker ps -a | grep brgm

echo ""
echo "2. Variables env chargées:"
docker exec brgm-dagster-webserver env | grep -E "DAGSTER_PG|MINIO"

echo ""
echo "3. Logs Dagster (10 dernières lignes):"
docker logs brgm-dagster-webserver --tail 10

echo ""
echo "4. Test port 8080:"
curl -I http://localhost:8080 2>&1 | head -5

