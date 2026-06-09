"""Create + replace gold.fct_monthly_index (per-station monthly index, fixed reference)."""

_CREATE = """
CREATE TABLE IF NOT EXISTS gold.fct_monthly_index (
    type        text NOT NULL,
    code        text NOT NULL,
    month       date NOT NULL,
    z           double precision,
    index_class text NOT NULL,
    flag        text,
    computed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (type, code, month)
);
CREATE INDEX IF NOT EXISTS idx_fct_monthly_index_type_month
    ON gold.fct_monthly_index (type, month);
"""

_INSERT = """
INSERT INTO gold.fct_monthly_index (code, type, month, z, index_class, flag, computed_at)
VALUES (%s, %s, %s, %s, %s, %s, now())
ON CONFLICT (type, code, month) DO UPDATE SET
    z = EXCLUDED.z,
    index_class = EXCLUDED.index_class,
    flag = EXCLUDED.flag,
    computed_at = now();
"""


def init_monthly_index_table(pg):
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS gold")
        cur.execute(_CREATE)
        conn.commit()


def upsert_monthly_index(pg, rows):
    """rows: list of (code, type, month_date, z|None, index_class, flag)."""
    if not rows:
        return
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.executemany(_INSERT, rows)
        conn.commit()
