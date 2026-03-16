"""pgvector CRUD for SoftCLT embeddings — upsert, search, schema init."""

import numpy as np
import logging

logger = logging.getLogger(__name__)

# SQL templates — parameterized by table/column names at call site
_CREATE_STATION_TABLE = """
CREATE TABLE IF NOT EXISTS ml.{domain}_station_embeddings (
    {id_col} TEXT NOT NULL,
    space TEXT NOT NULL DEFAULT 'multi',
    embedding vector(320) NOT NULL,
    cluster_id INT,
    model_version TEXT NOT NULL,
    n_days INT NOT NULL,
    n_windows INT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    umap_2d_x FLOAT,
    umap_2d_y FLOAT,
    umap_3d_x FLOAT,
    umap_3d_y FLOAT,
    umap_3d_z FLOAT,
    PRIMARY KEY ({id_col}, space)
)
"""

_CREATE_WINDOW_TABLE = """
CREATE TABLE IF NOT EXISTS ml.{domain}_window_embeddings (
    {id_col} TEXT NOT NULL,
    window_start DATE NOT NULL,
    space TEXT NOT NULL DEFAULT 'multi',
    window_end DATE NOT NULL,
    embedding vector(320) NOT NULL,
    model_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY ({id_col}, window_start, space)
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

_CREATE_CLUSTERING_RUNS = """
CREATE TABLE IF NOT EXISTS ml.clustering_runs (
    id SERIAL PRIMARY KEY,
    domain TEXT NOT NULL,
    level TEXT NOT NULL DEFAULT 'stations',
    space TEXT NOT NULL DEFAULT 'multi',
    method TEXT NOT NULL DEFAULT 'hdbscan',
    params JSONB NOT NULL,
    metrics JSONB NOT NULL,
    n_clusters INT NOT NULL,
    n_stations INT NOT NULL,
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

_CREATE_CLUSTERING_LABELS = """
CREATE TABLE IF NOT EXISTS ml.clustering_labels (
    run_id INT NOT NULL REFERENCES ml.clustering_runs(id) ON DELETE CASCADE,
    station_id TEXT NOT NULL,
    cluster_id INT NOT NULL,
    umap_2d_x FLOAT,
    umap_2d_y FLOAT,
    umap_3d_x FLOAT,
    umap_3d_y FLOAT,
    umap_3d_z FLOAT,
    PRIMARY KEY (run_id, station_id)
)
"""

_CREATE_CLUSTERING_LABELS_IDX = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_clustering_labels_run') THEN
        CREATE INDEX idx_clustering_labels_run ON ml.clustering_labels (run_id);
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

        cur.execute(_CREATE_CLUSTERING_RUNS)
        cur.execute(_CREATE_CLUSTERING_LABELS)
        cur.execute(_CREATE_CLUSTERING_LABELS_IDX)

        conn.commit()
    logger.info("ml schema and tables initialized")


def upsert_station_embeddings(pg, domain: str, id_col: str,
                              embeddings: dict[str, np.ndarray],
                              n_days: dict[str, int],
                              n_windows: dict[str, int],
                              version: str,
                              space: str = "multi"):
    """Upsert station embeddings into ml.{domain}_station_embeddings."""
    table = f"ml.{domain}_station_embeddings"
    with pg.get_connection() as conn:
        cur = conn.cursor()
        for sid, emb in embeddings.items():
            emb_str = "[" + ",".join(f"{v:.6f}" for v in emb) + "]"
            cur.execute(f"""
                INSERT INTO {table} ({id_col}, space, embedding, model_version, n_days, n_windows, updated_at)
                VALUES (%s, %s, %s::vector, %s, %s, %s, NOW())
                ON CONFLICT ({id_col}, space) DO UPDATE SET
                    embedding = EXCLUDED.embedding,
                    model_version = EXCLUDED.model_version,
                    n_days = EXCLUDED.n_days,
                    n_windows = EXCLUDED.n_windows,
                    updated_at = NOW()
            """, (sid, space, emb_str, version, n_days.get(sid, 0), n_windows.get(sid, 0)))
        conn.commit()
    logger.info(f"Upserted {len(embeddings)} {space} station embeddings into {table}")


def upsert_window_embeddings(pg, domain: str, id_col: str,
                             window_data: dict[str, tuple[np.ndarray, list[tuple[str, str]]]],
                             version: str,
                             space: str = "multi"):
    """Upsert window embeddings into ml.{domain}_window_embeddings."""
    table = f"ml.{domain}_window_embeddings"
    total = 0
    with pg.get_connection() as conn:
        cur = conn.cursor()
        for sid, (embs, date_ranges) in window_data.items():
            for emb, (start, end) in zip(embs, date_ranges):
                emb_str = "[" + ",".join(f"{v:.6f}" for v in emb) + "]"
                cur.execute(f"""
                    INSERT INTO {table} ({id_col}, window_start, space, window_end, embedding, model_version)
                    VALUES (%s, %s, %s, %s, %s::vector, %s)
                    ON CONFLICT ({id_col}, window_start, space) DO UPDATE SET
                        embedding = EXCLUDED.embedding,
                        window_end = EXCLUDED.window_end,
                        model_version = EXCLUDED.model_version
                """, (sid, start, space, end, emb_str, version))
                total += 1
        conn.commit()
    logger.info(f"Upserted {total} {space} window embeddings into {table}")


def update_umap_coords(pg, domain: str, id_col: str,
                       station_ids: list,
                       umap_2d: np.ndarray,
                       umap_3d: np.ndarray):
    """Update UMAP 2D and 3D coordinates for station embeddings.

    Args:
        pg: SQLAlchemy engine or psycopg2 connection wrapper with get_connection().
        domain: "piezo" or "hydro".
        id_col: "code_bss" or "code_station".
        station_ids: list of N station IDs.
        umap_2d: numpy array of shape (N, 2) — UMAP 2D coordinates.
        umap_3d: numpy array of shape (N, 3) — UMAP 3D coordinates.
    """
    table = f"ml.{domain}_station_embeddings"
    sql = f"""
        UPDATE {table}
        SET umap_2d_x = %s,
            umap_2d_y = %s,
            umap_3d_x = %s,
            umap_3d_y = %s,
            umap_3d_z = %s
        WHERE {id_col} = %s
    """
    with pg.get_connection() as conn:
        cur = conn.cursor()
        for i, sid in enumerate(station_ids):
            cur.execute(sql, (
                float(umap_2d[i, 0]), float(umap_2d[i, 1]),
                float(umap_3d[i, 0]), float(umap_3d[i, 1]), float(umap_3d[i, 2]),
                sid,
            ))
        conn.commit()
    logger.info(f"Updated UMAP coords for {len(station_ids)} stations in {table}")


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


def save_clustering_run(
    pg,
    domain: str,
    level: str,
    method: str,
    params: dict,
    metrics: dict,
    n_clusters: int,
    n_stations: int,
    is_default: bool = False,
    station_ids: list[str] | None = None,
    labels: np.ndarray | None = None,
    umap_2d: np.ndarray | None = None,
    umap_3d: np.ndarray | None = None,
    space: str = "multi",
) -> int:
    """Save a clustering run with labels and UMAP coords. Returns run_id."""
    import json

    with pg.get_connection() as conn:
        cur = conn.cursor()

        if is_default:
            cur.execute(
                "UPDATE ml.clustering_runs SET is_default = FALSE "
                "WHERE domain = %s AND level = %s AND space = %s AND is_default = TRUE",
                (domain, level, space),
            )

        cur.execute(
            """
            INSERT INTO ml.clustering_runs
                (domain, level, space, method, params, metrics, n_clusters, n_stations, is_default)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (domain, level, space, method,
             json.dumps(params), json.dumps(metrics),
             n_clusters, n_stations, is_default),
        )
        run_id = cur.fetchone()[0]

        if station_ids is not None and labels is not None:
            for i, (sid, label) in enumerate(zip(station_ids, labels)):
                u2x = float(umap_2d[i, 0]) if umap_2d is not None else None
                u2y = float(umap_2d[i, 1]) if umap_2d is not None else None
                u3x = float(umap_3d[i, 0]) if umap_3d is not None else None
                u3y = float(umap_3d[i, 1]) if umap_3d is not None else None
                u3z = float(umap_3d[i, 2]) if umap_3d is not None else None
                cur.execute(
                    """
                    INSERT INTO ml.clustering_labels
                        (run_id, station_id, cluster_id, umap_2d_x, umap_2d_y,
                         umap_3d_x, umap_3d_y, umap_3d_z)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (run_id, sid, int(label), u2x, u2y, u3x, u3y, u3z),
                )

        conn.commit()

    logger.info(
        "Saved clustering run %d: domain=%s level=%s method=%s "
        "n_clusters=%d n_stations=%d is_default=%s",
        run_id, domain, level, method, n_clusters, n_stations, is_default,
    )
    return run_id
