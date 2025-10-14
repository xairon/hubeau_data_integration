#!/bin/bash
# Script pour nettoyer les buckets MinIO depuis le container Docker

echo "🧹 Nettoyage des buckets MinIO..."
echo "=================================="

docker exec dagster python scripts/cleanup_minio_buckets.py

echo ""
echo "✅ Nettoyage terminé!"

