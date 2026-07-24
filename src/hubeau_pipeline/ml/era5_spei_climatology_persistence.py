"""Create + upsert gold.fct_era5_spei_climatology_grid (params de la logistique
généralisée (GLO) SPEI par cellule ERA5 × mois calendaire × fenêtre, référence
1991-2020).

Table Python-managée (pas dbt) : le fit L-moments a besoin des échantillons
annuels ET de la fonction Γ, hors de portée du SQL dbt.

Historique : la loi ajustée était une log-logistique (colonnes ll_alpha/ll_beta/
ll_gamma), remplacée par la GLO (glo_alpha/glo_k/glo_xi) car ~27% des mailles ont
une L-asymétrie τ₃ négative, hors du domaine de la log-logistique. Les colonnes
ll_* sont conservées (obsolètes, non détruites — table en prod) ; la migration
ADD COLUMN IF NOT EXISTS est idempotente.
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
    glo_alpha       double precision,
    glo_k           double precision,
    glo_xi          double precision,
    nb_annees       smallint,
    computed_at     timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (era5_latitude, era5_longitude, mois_calendaire, fenetre)
);
"""

# Migration idempotente pour la table déjà en prod (créée avec les seules
# colonnes ll_*) : ajoute les colonnes GLO sans toucher aux données existantes.
_ALTER_ADD_GLO = """
ALTER TABLE gold.fct_era5_spei_climatology_grid
    ADD COLUMN IF NOT EXISTS glo_alpha double precision;
ALTER TABLE gold.fct_era5_spei_climatology_grid
    ADD COLUMN IF NOT EXISTS glo_k double precision;
ALTER TABLE gold.fct_era5_spei_climatology_grid
    ADD COLUMN IF NOT EXISTS glo_xi double precision;
"""

_UPSERT = """
INSERT INTO gold.fct_era5_spei_climatology_grid
    (era5_latitude, era5_longitude, mois_calendaire, fenetre,
     glo_alpha, glo_k, glo_xi, nb_annees, computed_at)
VALUES %s
ON CONFLICT (era5_latitude, era5_longitude, mois_calendaire, fenetre) DO UPDATE SET
    glo_alpha = EXCLUDED.glo_alpha,
    glo_k     = EXCLUDED.glo_k,
    glo_xi    = EXCLUDED.glo_xi,
    nb_annees = EXCLUDED.nb_annees,
    computed_at = now();
"""

_TEMPLATE = "(%s, %s, %s, %s, %s, %s, %s, %s, now())"


def init_spei_climatology_table(pg):
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS gold")
        cur.execute(_CREATE)
        cur.execute(_ALTER_ADD_GLO)
        conn.commit()


def upsert_spei_climatology(pg, rows):
    """rows: iterable of (lat, lon, mois_calendaire, fenetre, alpha, k, xi, nb_annees)."""
    if not rows:
        return
    with pg.get_connection() as conn:
        cur = conn.cursor()
        execute_values(cur, _UPSERT, rows, template=_TEMPLATE, page_size=10_000)
        conn.commit()
