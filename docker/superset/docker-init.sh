#!/bin/bash
set -e

echo "🚀 Starting Superset Initialization..."

# 1. Wait for database
echo "⏳ Waiting for PostgreSQL..."
# Simple wait loop (could be improved with wait-for-it)
sleep 10

# 2. Database Upgrade (Migrations)
echo "📦 Running DB upgrades..."
superset db upgrade

# 3. Create Admin User (Idempotent-ish: will fail harmlessly if exists or we can check)
echo "bust_cache" | superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@fab.org \
    --password admin \
    || echo "User admin already exists (or creation failed)"

# 4. Init Roles
echo "🔑 Initializing roles..."
superset init

# 5. Import Datasources
echo "📥 Importing datasources from YAML..."
if [ -f /app/datasources.yaml ]; then
    superset import_datasources -p /app/datasources.yaml
    echo "✅ Datasources imported!"
else
    echo "⚠️ /app/datasources.yaml not found, skipping import."
fi

echo "✅ Initialization complete. Starting Server..."

# 6. Start Server (Original command)
/usr/bin/run-server.sh
