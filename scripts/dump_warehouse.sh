#!/usr/bin/env bash
# Dump the warehouse to a single restorable file.
#
# Produces a PostgreSQL custom-format dump: everything is preserved — hypertables,
# compression state, compression policies, PostGIS geometries, indexes, constraints.
# Restore it with scripts/restore_warehouse.sh on any host running the same
# timescaledb-ha:pg16 image.
#
# Usage:
#   bash scripts/dump_warehouse.sh [output_directory]
#
# Verified on TimescaleDB 2.29.2 with a compressed chunk present: pg_restore reported
# zero errors and the chunk came back still compressed.

set -euo pipefail

OUT_DIR="${1:-./backups}"
CONTAINER="${PG_CONTAINER:-brgm-postgres}"
DB="${PG_DB:-postgres}"
USER_="${PG_USER:-postgres}"
STAMP="$(date +%Y%m%d-%H%M%S)"
FILE="hubeau-warehouse-${STAMP}.dump"

mkdir -p "$OUT_DIR"

echo "==> Source"
docker exec "$CONTAINER" psql -U "$USER_" -d "$DB" -tAc \
  "SELECT '    database size: '||pg_size_pretty(pg_database_size('$DB'));"
docker exec "$CONTAINER" psql -U "$USER_" -d "$DB" -tAc \
  "SELECT '    '||hypertable_schema||'.'||hypertable_name||'  chunks='||num_chunks
   FROM timescaledb_information.hypertables ORDER BY 1;"

echo "==> Dumping (this reads the whole database; expect it to take a while on production)"
docker exec "$CONTAINER" pg_dump -U "$USER_" -Fc -f "/tmp/${FILE}" "$DB"

docker cp "${CONTAINER}:/tmp/${FILE}" "${OUT_DIR}/${FILE}"
docker exec "$CONTAINER" rm -f "/tmp/${FILE}"

SIZE=$(du -h "${OUT_DIR}/${FILE}" | cut -f1)
echo "==> Done: ${OUT_DIR}/${FILE}  (${SIZE})"
echo
echo "    Check it is readable before you trust it:"
echo "      pg_restore --list ${OUT_DIR}/${FILE} | head"
echo
echo "    A dump you have never restored is a hope, not a backup. Test it once:"
echo "      bash scripts/restore_warehouse.sh ${OUT_DIR}/${FILE} --into-throwaway"
