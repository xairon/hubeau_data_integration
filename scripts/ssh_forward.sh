#!/bin/bash
# SSH Port Forwarding Script - Hub'Eau Data Integration
# Convention: Local ports = Server ports with prefix '1'
#
# Usage:
#   ./scripts/ssh_forward.sh
#
# Then access:
#   - Dagster UI:  http://localhost:18080
#   - Adminer:     http://localhost:18081
#   - Portainer:   http://localhost:19000
#   - PostgreSQL:  localhost:15432

SERVER="dib-2019006065"
USER="ringuet"

echo "🔌 Starting SSH port forwarding to $SERVER..."
echo ""
echo "📋 Port mappings (Local → Remote):"
echo "   18080 → 8080  (Dagster UI)"
echo "   18081 → 8081  (Adminer - PostgreSQL Web UI)"
echo "   19000 → 9000  (Portainer - Docker Management)"
echo "   15432 → 5432  (PostgreSQL Direct Connection)"
echo ""
echo "🌐 Access URLs:"
echo "   Dagster:    http://localhost:18080"
echo "   Adminer:    http://localhost:18081"
echo "   Portainer:  http://localhost:19000"
echo ""
echo "🔐 PostgreSQL credentials:"
echo "   Host:     localhost:15432"
echo "   User:     postgres"
echo "   Password: REDACTED"
echo "   Database: postgres"
echo ""
echo "Press Ctrl+C to stop forwarding"
echo "================================================"
echo ""

ssh -L 18080:localhost:8080 \
    -L 18081:localhost:8081 \
    -L 19000:localhost:9000 \
    -L 15432:localhost:5432 \
    ${USER}@${SERVER}
