"""Create + upsert gold.station_current_index (per-station latest standardized index)."""

_CREATE = """
CREATE TABLE IF NOT EXISTS gold.station_current_index (
    code            text NOT NULL,
    type            text NOT NULL,
    index_name      text NOT NULL,
    index_value     double precision,
    index_class     text NOT NULL,
    ref_month       date,
    baseline_start  date,
    baseline_end    date,
    computed_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (type, code)
);
CREATE INDEX IF NOT EXISTS idx_station_current_index_class
    ON gold.station_current_index (index_class);
"""

_UPSERT = """
INSERT INTO gold.station_current_index
    (code, type, index_name, index_value, index_class, ref_month, baseline_start, baseline_end, computed_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (type, code) DO UPDATE SET
    index_name = EXCLUDED.index_name,
    index_value = EXCLUDED.index_value,
    index_class = EXCLUDED.index_class,
    ref_month = EXCLUDED.ref_month,
    baseline_start = EXCLUDED.baseline_start,
    baseline_end = EXCLUDED.baseline_end,
    computed_at = now();
"""


def init_current_index_table(pg):
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS gold")
        cur.execute(_CREATE)
        conn.commit()


def upsert_current_index(pg, rows):
    """rows: list of (code, type, index_name, index_value|None, index_class, ref_month, baseline_start, baseline_end)."""
    if not rows:
        return
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.executemany(_UPSERT, rows)
        conn.commit()
