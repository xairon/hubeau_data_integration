#!/bin/bash
# Script de debug pour comprendre pourquoi le worker est unhealthy

echo "🔍 Debug Worker Health Status"
echo "=============================="
echo ""

# 1. Status des conteneurs
echo "1. Status de tous les conteneurs:"
docker compose -f /srv/brgm/docker-compose.production.yml ps

echo ""
echo "2. Logs du worker (50 dernières lignes):"
docker logs brgm-dlt-worker --tail 50

echo ""
echo "3. Health check configuration du worker:"
docker inspect brgm-dlt-worker --format='{{json .State.Health}}' | python3 -m json.tool

echo ""
echo "4. Variables d'environnement dans le worker:"
docker exec brgm-dlt-worker env | grep -E '(PG_|DAGSTER_|MINIO_|AWS_)' | sort

echo ""
echo "5. Test de connexion PostgreSQL depuis le worker:"
docker exec brgm-dlt-worker python -c "
import os
import psycopg2
try:
    conn = psycopg2.connect(
        host=os.getenv('PG_HOST', 'postgres'),
        port=5432,
        dbname='hubeau',
        user='postgres',
        password=os.getenv('PG_PASSWORD')
    )
    print('✅ PostgreSQL connection OK')
    conn.close()
except Exception as e:
    print(f'❌ PostgreSQL connection failed: {e}')
"

echo ""
echo "6. Test de connexion Dagster PostgreSQL depuis le worker:"
docker exec brgm-dlt-worker python -c "
import os
import psycopg2
try:
    conn = psycopg2.connect(
        host=os.getenv('DAGSTER_PG_HOST', 'dagster_postgres'),
        port=5432,
        dbname=os.getenv('DAGSTER_PG_DB', 'dagster'),
        user=os.getenv('DAGSTER_PG_USER', 'postgres'),
        password=os.getenv('DAGSTER_PG_PASSWORD')
    )
    print('✅ Dagster PostgreSQL connection OK')
    conn.close()
except Exception as e:
    print(f'❌ Dagster PostgreSQL connection failed: {e}')
"

echo ""
echo "7. Network connectivity:"
docker exec brgm-dlt-worker ping -c 2 dagster_postgres || echo "❌ Cannot ping dagster_postgres"
docker exec brgm-dlt-worker ping -c 2 postgres || echo "❌ Cannot ping postgres"