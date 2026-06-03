"""Create + upsert gold.station_reference_stats (per-station per-month reference grid)."""
import json

_CREATE = """
CREATE TABLE IF NOT EXISTS gold.station_reference_stats (
    type            text NOT NULL,
    code            text NOT NULL,
    month           int  NOT NULL,
    quantile_grid   jsonb,
    baseline_start  date,
    baseline_end    date,
    flag            text NOT NULL,
    n_years         int,
    computed_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (type, code, month)
);
"""

_UPSERT = """
INSERT INTO gold.station_reference_stats
    (type, code, month, quantile_grid, baseline_start, baseline_end, flag, n_years, computed_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (type, code, month) DO UPDATE SET
    quantile_grid = EXCLUDED.quantile_grid,
    baseline_start = EXCLUDED.baseline_start,
    baseline_end = EXCLUDED.baseline_end,
    flag = EXCLUDED.flag,
    n_years = EXCLUDED.n_years,
    computed_at = now();
"""


def init_reference_stats_table(pg):
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS gold")
        cur.execute(_CREATE)
        conn.commit()


def upsert_reference_stats(pg, rows):
    """rows: list of (type, code, month, grid_list|None, baseline_start, baseline_end, flag, n_years).

    grid_list is JSON-serialised here (jsonb column).
    """
    if not rows:
        return
    payload = [
        (t, c, m, json.dumps(g) if g is not None else None, bs, be, flag, ny)
        for (t, c, m, g, bs, be, flag, ny) in rows
    ]
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.executemany(_UPSERT, payload)
        conn.commit()
