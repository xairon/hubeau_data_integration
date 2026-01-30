"""
BDLISA CSV Assets - chargement des tables CSV nationales (hors TME)

Tables attendues dans un dossier CSV local (ex. D:/BDLISA_V3_NATIONAL-csv(1)/CSV):
- TABLE_GENEALOGIE.csv
- TABLE_LITHOLOGIE_NIV1.csv / NIV2 / NIV3
- TABLE_PILE_ENTITES_NIV1.csv / NIV2 / NIV3
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
from dagster import AssetExecutionContext, asset
from psycopg2.extras import execute_values

from hubeau_pipeline.resources import PostgreSQLResource


def _normalize_col(name: str) -> str:
    return (
        str(name)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
    )


def _detect_sep(path: Path) -> str:
    line = path.read_text(encoding="utf-8", errors="ignore").splitlines()[0]
    if line.count(";") >= line.count(","):
        return ";"
    return ","


def _iter_csv_chunks(path: Path, sep: str) -> Iterable[pd.DataFrame]:
    return pd.read_csv(
        path,
        sep=sep,
        dtype=str,
        keep_default_na=False,
        chunksize=50000,
        encoding="utf-8",
        engine="python",
    )


def _load_csv_to_table(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
    path: Path,
    table_name: str,
) -> int:
    sep = _detect_sep(path)
    rows_inserted = 0
    columns: Optional[List[str]] = None

    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
            cur.execute(f"DROP TABLE IF EXISTS bronze.{table_name}")

    for chunk in _iter_csv_chunks(path, sep):
        if columns is None:
            columns = [_normalize_col(c) for c in list(chunk.columns)]
            col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
            with pg.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"CREATE TABLE bronze.{table_name} ({col_defs})")
                conn.commit()

        chunk.columns = columns
        values = [tuple(row) for row in chunk.itertuples(index=False, name=None)]
        if values:
            with pg.get_connection() as conn:
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        f'INSERT INTO bronze.{table_name} ({",".join(chr(34) + c + chr(34) for c in columns)}) VALUES %s',
                        values,
                    )
                conn.commit()
            rows_inserted += len(values)

    context.log.info("%s : %s lignes", table_name, rows_inserted)
    return rows_inserted


def _csv_dir() -> Path:
    env_dir = os.environ.get("BDLISA_CSV_DIR")
    if env_dir:
        return Path(env_dir)
    return Path("D:/BDLISA_V3_NATIONAL-csv(1)/CSV")


@asset(description="Table généalogie BDLISA (CSV national)", group_name="bronze")
def bdlisa_table_genealogie(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
) -> dict:
    path = _csv_dir() / "TABLE_GENEALOGIE.csv"
    if not path.exists():
        context.log.warning("Fichier manquant: %s", path)
        return {"rows": 0, "path": str(path)}
    rows = _load_csv_to_table(context, pg, path, "bdlisa_table_genealogie")
    return {"rows": rows, "path": str(path)}


@asset(description="Table lithologie niveau 1 (CSV national)", group_name="bronze")
def bdlisa_lithologie_niv1(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
) -> dict:
    path = _csv_dir() / "TABLE_LITHOLOGIE_NIV1.csv"
    if not path.exists():
        context.log.warning("Fichier manquant: %s", path)
        return {"rows": 0, "path": str(path)}
    rows = _load_csv_to_table(context, pg, path, "bdlisa_lithologie_niv1")
    return {"rows": rows, "path": str(path)}


@asset(description="Table lithologie niveau 2 (CSV national)", group_name="bronze")
def bdlisa_lithologie_niv2(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
) -> dict:
    path = _csv_dir() / "TABLE_LITHOLOGIE_NIV2.csv"
    if not path.exists():
        context.log.warning("Fichier manquant: %s", path)
        return {"rows": 0, "path": str(path)}
    rows = _load_csv_to_table(context, pg, path, "bdlisa_lithologie_niv2")
    return {"rows": rows, "path": str(path)}


@asset(description="Table lithologie niveau 3 (CSV national)", group_name="bronze")
def bdlisa_lithologie_niv3(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
) -> dict:
    path = _csv_dir() / "TABLE_LITHOLOGIE_NIV3.csv"
    if not path.exists():
        context.log.warning("Fichier manquant: %s", path)
        return {"rows": 0, "path": str(path)}
    rows = _load_csv_to_table(context, pg, path, "bdlisa_lithologie_niv3")
    return {"rows": rows, "path": str(path)}


@asset(description="Table pile entités niveau 1 (CSV national)", group_name="bronze")
def bdlisa_pile_entites_niv1(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
) -> dict:
    path = _csv_dir() / "TABLE_PILE_ENTITES_NIV1.csv"
    if not path.exists():
        context.log.warning("Fichier manquant: %s", path)
        return {"rows": 0, "path": str(path)}
    rows = _load_csv_to_table(context, pg, path, "bdlisa_pile_entites_niv1")
    return {"rows": rows, "path": str(path)}


@asset(description="Table pile entités niveau 2 (CSV national)", group_name="bronze")
def bdlisa_pile_entites_niv2(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
) -> dict:
    path = _csv_dir() / "TABLE_PILE_ENTITES_NIV2.csv"
    if not path.exists():
        context.log.warning("Fichier manquant: %s", path)
        return {"rows": 0, "path": str(path)}
    rows = _load_csv_to_table(context, pg, path, "bdlisa_pile_entites_niv2")
    return {"rows": rows, "path": str(path)}


@asset(description="Table pile entités niveau 3 (CSV national)", group_name="bronze")
def bdlisa_pile_entites_niv3(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
) -> dict:
    path = _csv_dir() / "TABLE_PILE_ENTITES_NIV3.csv"
    if not path.exists():
        context.log.warning("Fichier manquant: %s", path)
        return {"rows": 0, "path": str(path)}
    rows = _load_csv_to_table(context, pg, path, "bdlisa_pile_entites_niv3")
    return {"rows": rows, "path": str(path)}
