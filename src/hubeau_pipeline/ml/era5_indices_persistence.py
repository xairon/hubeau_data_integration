"""Create + upsert gold.fct_era5_indices_grid (SPI/STI par cellule ERA5, fenêtres 1/3/6/12)."""
from psycopg2.extras import execute_values

_CREATE = """
CREATE TABLE IF NOT EXISTS gold.fct_era5_indices_grid (
    era5_latitude  numeric(6,3) NOT NULL,
    era5_longitude numeric(6,3) NOT NULL,
    month          date NOT NULL,
    fenetre        smallint NOT NULL,
    spi            double precision,
    sti            double precision,
    spei           double precision,
    computed_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (era5_latitude, era5_longitude, month, fenetre)
);
CREATE INDEX IF NOT EXISTS idx_fct_era5_indices_grid_month
    ON gold.fct_era5_indices_grid (month, fenetre);
"""

_ALTER_ADD_SPEI = """
ALTER TABLE gold.fct_era5_indices_grid ADD COLUMN IF NOT EXISTS spei double precision;
"""

_UPSERT = """
INSERT INTO gold.fct_era5_indices_grid
    (era5_latitude, era5_longitude, month, fenetre, spi, sti, spei, computed_at)
VALUES %s
ON CONFLICT (era5_latitude, era5_longitude, month, fenetre) DO UPDATE SET
    spi = EXCLUDED.spi,
    sti = EXCLUDED.sti,
    spei = EXCLUDED.spei,
    computed_at = now();
"""

_TEMPLATE = "(%s, %s, %s, %s, %s, %s, %s, now())"


def init_era5_indices_table(pg):
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS gold")
        cur.execute(_CREATE)
        cur.execute(_ALTER_ADD_SPEI)   # colonne ajoutée sur une table déjà en prod
        conn.commit()


def upsert_era5_indices(pg, rows):
    """rows: iterable of (lat, lon, month_date, fenetre, spi|None, sti|None, spei|None)."""
    if not rows:
        return
    with pg.get_connection() as conn:
        cur = conn.cursor()
        execute_values(cur, _UPSERT, rows, template=_TEMPLATE, page_size=10_000)
        conn.commit()


def latest_index_month(pg):
    """Dernier mois indexé, ou None si la table est vide (déclenche le bootstrap complet)."""
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT MAX(month) FROM gold.fct_era5_indices_grid")
        return cur.fetchone()[0]
