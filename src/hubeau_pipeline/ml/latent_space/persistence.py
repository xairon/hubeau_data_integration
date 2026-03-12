"""pgvector CRUD for SoftCLT embeddings — upsert, search, schema init."""

import numpy as np
import logging

logger = logging.getLogger(__name__)

# SQL templates — parameterized by table/column names at call site
_CREATE_STATION_TABLE = """
CREATE TABLE IF NOT EXISTS ml.{domain}_station_embeddings (
    {id_col} TEXT PRIMARY KEY,
    embedding vector(320) NOT NULL,
    cluster_id INT,
    model_version TEXT NOT NULL,
    n_days INT NOT NULL,
    n_windows INT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

_CREATE_WINDOW_TABLE = """
CREATE TABLE IF NOT EXISTS ml.{domain}_window_embeddings (
    {id_col} TEXT NOT NULL,
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    embedding vector(320) NOT NULL,
    model_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY ({id_col}, window_start)
)
"""

_CREATE_HNSW_INDEX = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = '{idx_name}') THEN
        CREATE INDEX {idx_name}
            ON ml.{domain}_station_embeddings
            USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);
    END IF;
END $$;
"""

_CREATE_BTREE_INDEX = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = '{idx_name}') THEN
        CREATE INDEX {idx_name}
            ON ml.{domain}_window_embeddings ({id_col}, window_start);
    END IF;
END $$;
"""


def init_ml_schema(pg):
    """Create ml schema, pgvector extension, and all 4 tables + indexes (idempotent)."""
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute("CREATE SCHEMA IF NOT EXISTS ml")

        for domain, id_col in [("piezo", "code_bss"), ("hydro", "code_station")]:
            cur.execute(_CREATE_STATION_TABLE.format(domain=domain, id_col=id_col))
            cur.execute(_CREATE_WINDOW_TABLE.format(domain=domain, id_col=id_col))
            cur.execute(_CREATE_HNSW_INDEX.format(
                idx_name=f"idx_{domain}_station_emb_hnsw", domain=domain
            ))
            cur.execute(_CREATE_BTREE_INDEX.format(
                idx_name=f"idx_{domain}_window_station", domain=domain, id_col=id_col
            ))

        conn.commit()
    logger.info("ml schema and tables initialized")


def upsert_station_embeddings(pg, domain: str, id_col: str,
                              embeddings: dict[str, np.ndarray],
                              n_days: dict[str, int],
                              n_windows: dict[str, int],
                              version: str):
    """Upsert station embeddings into ml.{domain}_station_embeddings."""
    table = f"ml.{domain}_station_embeddings"
    with pg.get_connection() as conn:
        cur = conn.cursor()
        for sid, emb in embeddings.items():
            emb_str = "[" + ",".join(f"{v:.6f}" for v in emb) + "]"
            cur.execute(f"""
                INSERT INTO {table} ({id_col}, embedding, model_version, n_days, n_windows, updated_at)
                VALUES (%s, %s::vector, %s, %s, %s, NOW())
                ON CONFLICT ({id_col}) DO UPDATE SET
                    embedding = EXCLUDED.embedding,
                    model_version = EXCLUDED.model_version,
                    n_days = EXCLUDED.n_days,
                    n_windows = EXCLUDED.n_windows,
                    updated_at = NOW()
            """, (sid, emb_str, version, n_days.get(sid, 0), n_windows.get(sid, 0)))
        conn.commit()
    logger.info(f"Upserted {len(embeddings)} station embeddings into {table}")


def upsert_window_embeddings(pg, domain: str, id_col: str,
                             window_data: dict[str, tuple[np.ndarray, list[tuple[str, str]]]],
                             version: str):
    """Upsert window embeddings into ml.{domain}_window_embeddings."""
    table = f"ml.{domain}_window_embeddings"
    total = 0
    with pg.get_connection() as conn:
        cur = conn.cursor()
        for sid, (embs, date_ranges) in window_data.items():
            for emb, (start, end) in zip(embs, date_ranges):
                emb_str = "[" + ",".join(f"{v:.6f}" for v in emb) + "]"
                cur.execute(f"""
                    INSERT INTO {table} ({id_col}, window_start, window_end, embedding, model_version)
                    VALUES (%s, %s, %s, %s::vector, %s)
                    ON CONFLICT ({id_col}, window_start) DO UPDATE SET
                        embedding = EXCLUDED.embedding,
                        window_end = EXCLUDED.window_end,
                        model_version = EXCLUDED.model_version
                """, (sid, start, end, emb_str, version))
                total += 1
        conn.commit()
    logger.info(f"Upserted {total} window embeddings into {table}")


def search_similar(pg, domain: str, id_col: str, station_id: str, k: int = 10) -> list[dict]:
    """Find k most similar stations by cosine distance (HNSW)."""
    table = f"ml.{domain}_station_embeddings"
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT {id_col}, embedding <=> (
                SELECT embedding FROM {table} WHERE {id_col} = %s
            ) AS distance
            FROM {table}
            WHERE {id_col} != %s
            ORDER BY distance LIMIT %s
        """, (station_id, station_id, k))
        return [{id_col: r[0], "distance": float(r[1])} for r in cur.fetchall()]
