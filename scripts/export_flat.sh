#!/usr/bin/env bash
# Export the Gold layer as flat files, for people who will not run PostgreSQL.
#
# Usage:
#   bash scripts/export_flat.sh [output_directory] [format]
#     format: tsv (default, zero dependency) | parquet (needs duckdb on the host)
#
# What this is NOT: a backup. Flat files lose hypertables, indexes, PostGIS geometry
# types and the ability to restart the pipeline. Use scripts/dump_warehouse.sh for that.
# This is a delivery format — readable by pandas, R, DuckDB, Excel, anything.

set -euo pipefail

OUT_DIR="${1:-./export}"
FORMAT="${2:-tsv}"
CONTAINER="${PG_CONTAINER:-brgm-postgres}"
mkdir -p "$OUT_DIR"

TABLES=$(docker exec "$CONTAINER" psql -U postgres -d postgres -tAc "
SELECT schemaname||'.'||relname
FROM pg_stat_user_tables
WHERE schemaname = 'gold' AND n_live_tup > 0
ORDER BY relname;")

[ -n "$TABLES" ] || { echo "Nothing in the gold schema — has dbt run?" >&2; exit 1; }

case "$FORMAT" in
tsv)
    for t in $TABLES; do
        name="${t#gold.}"
        echo "==> ${t}"
        # Geometry columns are emitted as WKB hex by default; ST_AsText would be friendlier
        # but changes the column type, so it is left to the consumer.
        docker exec "$CONTAINER" psql -U postgres -d postgres -c \
            "\\copy (SELECT * FROM ${t}) TO STDOUT WITH (FORMAT csv, DELIMITER E'\\t', HEADER)" \
            | gzip > "${OUT_DIR}/${name}.tsv.gz"
        echo "    $(du -h "${OUT_DIR}/${name}.tsv.gz" | cut -f1)"
    done
    ;;
parquet)
    command -v duckdb >/dev/null || {
        echo "duckdb not found. Install it (single binary, https://duckdb.org) or use tsv." >&2
        exit 1; }
    PGPW=$(grep -E '^PG_PASSWORD=' .env | cut -d= -f2-)
    for t in $TABLES; do
        name="${t#gold.}"
        echo "==> ${t}"
        duckdb -c "
          INSTALL postgres; LOAD postgres;
          ATTACH 'host=127.0.0.1 port=49502 user=postgres password=${PGPW} dbname=postgres' AS pg (TYPE postgres, READ_ONLY);
          COPY (SELECT * FROM pg.${t}) TO '${OUT_DIR}/${name}.parquet' (FORMAT parquet, COMPRESSION zstd);"
        echo "    $(du -h "${OUT_DIR}/${name}.parquet" | cut -f1)"
    done
    ;;
*)
    echo "Unknown format: ${FORMAT} (tsv | parquet)" >&2; exit 1 ;;
esac

echo "==> Written to ${OUT_DIR}"
