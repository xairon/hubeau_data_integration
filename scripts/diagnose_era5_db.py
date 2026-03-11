#!/usr/bin/env python3
"""
Diagnose ERA5 tables in Postgres
===============================

Checks:
- indexes on staging.era5_france_timeseries
- whether year 2006 exists in staging.era5_france_timeseries
- whether the raw NetCDF chunk covering 2006 exists in staging.era5_france_meteo_raw

Usage (PowerShell):
  python scripts/diagnose_era5_db.py --host localhost --port 5432 --user postgres --password $PG_PASSWORD --db postgres --year 2006
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from typing import Iterable

import psycopg


@dataclass(frozen=True)
class DbCfg:
    host: str
    port: int
    db: str
    user: str
    password: str


def _connect(cfg: DbCfg) -> psycopg.Connection:
    return psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.db,
        user=cfg.user,
        password=cfg.password,
        connect_timeout=10,
    )


def _print_rows(rows: Iterable[tuple], indent: str = "  ") -> None:
    printed = False
    for r in rows:
        printed = True
        print(f"{indent}- {r}")
    if not printed:
        print(f"{indent}(aucune ligne)")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--host", required=True)
    p.add_argument("--port", required=True, type=int)
    p.add_argument("--db", default="postgres")
    p.add_argument("--user", default="postgres")
    p.add_argument("--password", required=True)
    p.add_argument("--schema", default="staging")
    p.add_argument("--year", type=int, default=2006)
    args = p.parse_args()

    cfg = DbCfg(host=args.host, port=args.port, db=args.db, user=args.user, password=args.password)
    schema = args.schema
    year = args.year

    t0 = date(year, 1, 1)
    t1 = date(year + 1, 1, 1)

    print(f"== Connexion: {cfg.host}:{cfg.port}/{cfg.db} user={cfg.user} ==")
    with _connect(cfg) as conn:
        with conn.cursor() as cur:
            print("\n== Index sur staging.era5_france_timeseries ==")
            cur.execute(
                """
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = %s AND tablename = 'era5_france_timeseries'
                ORDER BY indexname
                """,
                (schema,),
            )
            _print_rows(cur.fetchall())

            print(f"\n== Présence de l'année {year} dans staging.era5_france_timeseries ==")
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM staging.era5_france_timeseries
                    WHERE time >= %s AND time < %s
                    LIMIT 1
                )
                """,
                (t0, t1),
            )
            has_2006 = cur.fetchone()[0]
            print(f"  exists(time in [{t0}, {t1}[) = {has_2006}")

            print("\n== Chunk(s) raw couvrant l'année (staging.era5_france_meteo_raw) ==")
            cur.execute(
                """
                SELECT file_id, start_year, end_year, file_size_mb, download_timestamp
                FROM staging.era5_france_meteo_raw
                WHERE start_year <= %s AND end_year >= %s
                ORDER BY start_year
                """,
                (year, year),
            )
            raw_chunks = cur.fetchall()
            _print_rows(raw_chunks)

            if raw_chunks:
                # Pick the first matching chunk for detailed check in timeseries.
                file_id = raw_chunks[0][0]
                print(f"\n== Vérif extraction timeseries du chunk {file_id} ==")
                cur.execute(
                    """
                    SELECT COUNT(*)::bigint, MIN(time), MAX(time)
                    FROM staging.era5_france_timeseries
                    WHERE source_file_id = %s
                    """,
                    (file_id,),
                )
                cnt, mn, mx = cur.fetchone()
                print(f"  rows for {file_id}: {cnt:,} (min={mn}, max={mx})")

    print("\n✅ Diagnostic terminé.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

