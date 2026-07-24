"""Create + upsert gold.fct_era5_spei_climatology_grid (params log-logistiques
SPEI par cellule ERA5 × mois calendaire × fenêtre, référence 1991-2020).

Table Python-managée (pas dbt) : le fit L-moments a besoin des échantillons
annuels ET de la fonction Γ, hors de portée du SQL dbt.
"""
from psycopg2.extras import execute_values

_CREATE = """
CREATE TABLE IF NOT EXISTS gold.fct_era5_spei_climatology_grid (
    era5_latitude   numeric(6,3) NOT NULL,
    era5_longitude  numeric(6,3) NOT NULL,
    mois_calendaire smallint     NOT NULL,
    fenetre         smallint     NOT NULL,
    ll_alpha        double precision,
    ll_beta         double precision,
    ll_gamma        double precision,
    nb_annees       smallint,
    computed_at     timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (era5_latitude, era5_longitude, mois_calendaire, fenetre)
);
"""

_UPSERT = """
INSERT INTO gold.fct_era5_spei_climatology_grid
    (era5_latitude, era5_longitude, mois_calendaire, fenetre,
     ll_alpha, ll_beta, ll_gamma, nb_annees, computed_at)
VALUES %s
ON CONFLICT (era5_latitude, era5_longitude, mois_calendaire, fenetre) DO UPDATE SET
    ll_alpha = EXCLUDED.ll_alpha,
    ll_beta  = EXCLUDED.ll_beta,
    ll_gamma = EXCLUDED.ll_gamma,
    nb_annees = EXCLUDED.nb_annees,
    computed_at = now();
"""

_TEMPLATE = "(%s, %s, %s, %s, %s, %s, %s, %s, now())"


def init_spei_climatology_table(pg):
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS gold")
        cur.execute(_CREATE)
        conn.commit()


def upsert_spei_climatology(pg, rows):
    """rows: iterable of (lat, lon, mois_calendaire, fenetre, alpha, beta, gamma, nb_annees)."""
    if not rows:
        return
    with pg.get_connection() as conn:
        cur = conn.cursor()
        execute_values(cur, _UPSERT, rows, template=_TEMPLATE, page_size=10_000)
        conn.commit()
