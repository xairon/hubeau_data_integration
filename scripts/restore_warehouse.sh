#!/usr/bin/env bash
# Restore a warehouse dump produced by scripts/dump_warehouse.sh.
#
# Usage:
#   bash scripts/restore_warehouse.sh <dump-file>                    # into the running stack
#   bash scripts/restore_warehouse.sh <dump-file> --into-throwaway   # into a temporary container
#
# The throwaway mode is how you verify a dump without touching anything: it starts a
# disposable timescaledb container, restores into it, reports the row counts, and removes it.

set -euo pipefail

DUMP="${1:?usage: restore_warehouse.sh <dump-file> [--into-throwaway]}"
MODE="${2:-}"
[ -f "$DUMP" ] || { echo "No such file: $DUMP" >&2; exit 1; }

if [ "$MODE" = "--into-throwaway" ]; then
    NAME="hubeau-restore-test-$$"
    echo "==> Starting throwaway container ${NAME}"
    docker run -d --name "$NAME" -e POSTGRES_PASSWORD=throwaway \
        timescale/timescaledb-ha:pg16 >/dev/null
    trap 'echo "==> Removing ${NAME}"; docker rm -f "$NAME" >/dev/null' EXIT
    until docker exec "$NAME" pg_isready -U postgres >/dev/null 2>&1; do sleep 2; done
    TARGET="$NAME"
else
    TARGET="${PG_CONTAINER:-brgm-postgres}"
    echo "==> Restoring into the live container ${TARGET}."
    echo "    This writes over existing objects. Ctrl-C now if that is not what you want."
    sleep 5
fi

docker cp "$DUMP" "${TARGET}:/tmp/restore.dump"
docker exec "$TARGET" psql -U postgres -d postgres -qc \
    "CREATE EXTENSION IF NOT EXISTS timescaledb;" >/dev/null

# timescaledb_pre/post_restore() are the officially supported way to bracket a restore.
# They are a no-op-ish safety on 2.29 (a plain pg_restore already works), but they are
# required on older versions and cost nothing here.
docker exec "$TARGET" psql -U postgres -d postgres -qc "SELECT timescaledb_pre_restore();" >/dev/null
set +e
docker exec "$TARGET" pg_restore -U postgres -d postgres /tmp/restore.dump 2>/tmp/restore.err
RC=$?
set -e
docker exec "$TARGET" psql -U postgres -d postgres -qc "SELECT timescaledb_post_restore();" >/dev/null
docker exec "$TARGET" rm -f /tmp/restore.dump

echo "==> pg_restore exit code: ${RC} (non-zero can still mean a usable restore; read the errors)"
grep -c '^pg_restore: error' /tmp/restore.err 2>/dev/null | sed 's/^/    errors: /' || true

echo "==> What came back"
docker exec "$TARGET" psql -U postgres -d postgres -c "
SELECT schemaname, relname AS table_name, n_live_tup AS rows
FROM pg_stat_user_tables
WHERE schemaname IN ('bronze','silver','gold') AND n_live_tup > 0
ORDER BY n_live_tup DESC LIMIT 15;"
docker exec "$TARGET" psql -U postgres -d postgres -c "
SELECT hypertable_name, num_chunks, compression_enabled
FROM timescaledb_information.hypertables ORDER BY 1;"
