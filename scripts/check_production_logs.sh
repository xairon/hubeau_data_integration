#!/bin/bash
# Script to check production logs on the server

echo "==================================="
echo "Checking DLT Worker Status"
echo "==================================="
echo ""

echo "Container status:"
docker ps -a | grep brgm-dlt-worker

echo ""
echo "Last 50 lines of worker logs:"
docker logs brgm-dlt-worker --tail 50

echo ""
echo "==================================="
echo "Checking Worker Health"
echo "==================================="
docker inspect brgm-dlt-worker --format='{{json .State.Health}}' | python3 -m json.tool

echo ""
echo "==================================="
echo "Checking if port 4000 is listening"
echo "==================================="
docker exec brgm-dlt-worker netstat -tlnp 2>/dev/null || echo "netstat not available"
docker exec brgm-dlt-worker ss -tlnp 2>/dev/null || echo "ss not available"

echo ""
echo "==================================="
echo "Testing GRPC connection"
echo "==================================="
docker exec brgm-dlt-worker python -c "import socket; s=socket.socket(); s.settimeout(5); s.connect(('localhost', 4000)); print('✓ Port 4000 reachable'); s.close()" 2>&1

echo ""
echo "==================================="
echo "Environment variables in worker"
echo "==================================="
docker exec brgm-dlt-worker env | grep -E "DAGSTER|PYTHONPATH|MINIO"
