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

# 3. Create Admin User
ADMIN_USER=${SUPERSET_ADMIN_USER:-admin}
ADMIN_PASSWORD=${SUPERSET_ADMIN_PASSWORD:-admin}
ADMIN_EMAIL=${SUPERSET_ADMIN_EMAIL:-admin@hubeau.com}

echo "bust_cache" | superset fab create-admin \
    --username "$ADMIN_USER" \
    --firstname Admin \
    --lastname User \
    --email "$ADMIN_EMAIL" \
    --password "$ADMIN_PASSWORD" \
    || echo "User $ADMIN_USER already exists (or creation failed)"

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
