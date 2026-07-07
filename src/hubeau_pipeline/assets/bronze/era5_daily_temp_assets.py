"""
ERA5 Daily Temperature Stats Bronze Layer Assets (derived-era5-land-daily-statistics)

Miroir de `era5_assets.py` (client CADS, retry/backoff, DELETE overlap + INSERT
idempotent, hypertable, conversion K->degC) pour le dataset CDS
`derived-era5-land-daily-statistics` : moyenne/min/max journalières de
`2m_temperature`, calculées côté CADS sur les 24 pas horaires.

Différence structurante vs l'ingestion horaire existante : le paramètre
`daily_statistic` est un STRING simple (pas une liste) -> 3 requêtes CDS
séparées par fenêtre (une par statistique), dont les résultats sont fusionnés
en mémoire sur (time, latitude, longitude) avant insertion.

Entrées normatives :
- Spec design : docs/superpowers/specs/2026-07-07-era5-daily-temperature-stats-design.md
- Spike CDS   : .superpowers/sdd/spike-cds-daily-stats.md (requête canonique,
  structure NetCDF, gotchas `number`/`valid_time`/Kelvin/NaN=mer)
- Référence   : src/hubeau_pipeline/assets/bronze/era5_assets.py

Ce module N'EST PAS câblé dans assets/__init__.py ni les jobs (voir Task T3
du plan docs/superpowers/plans/2026-07-07-era5-daily-temp-ingestion.md).
"""

import gc
import logging
import os
import tempfile
import zipfile
from datetime import datetime, timedelta

import cdsapi
import pandas as pd
import psycopg2
import xarray as xr
import yaml
from dagster import AssetExecutionContext, MetadataValue, Output, StaticPartitionsDefinition, asset
from psycopg2.extras import execute_values

CONFIG_PATH = "configs/era5/era5_daily_temp_stats.yml"

# ============================================================================
# PARTITIONS - ERA5 daily temp stats (chunks de 1 an, mirroir de era5_assets.py)
# ============================================================================

ERA5_DAILY_TEMP_START_YEAR = 1950  # ERA5-Land commence en 1950
ERA5_DAILY_TEMP_YEARS_PER_CHUNK = 1

# +1 buffer year to handle year rollover in long-running daemons (même pattern
# que ERA5_PARTITIONS dans era5_assets.py)
_current_year = datetime.now().year
ERA5_DAILY_TEMP_PARTITIONS = []
_year = ERA5_DAILY_TEMP_START_YEAR
while _year <= _current_year + 1:
    _chunk_end = min(_year + ERA5_DAILY_TEMP_YEARS_PER_CHUNK - 1, _current_year + 1)
    ERA5_DAILY_TEMP_PARTITIONS.append(f"{_year}_{_chunk_end}")
    _year += ERA5_DAILY_TEMP_YEARS_PER_CHUNK

ERA5_DAILY_TEMP_PARTITIONS_DEF = StaticPartitionsDefinition(ERA5_DAILY_TEMP_PARTITIONS)


# ============================================================================
# SHARED HELPERS
# ============================================================================

def _ensure_table(conn):
    """Create bronze.era5_daily_temp_stats if not exists (idempotent)."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze.era5_daily_temp_stats (
                id BIGSERIAL,
                time TIMESTAMP NOT NULL,
                latitude NUMERIC(6,3) NOT NULL,
                longitude NUMERIC(6,3) NOT NULL,
                t2m_mean NUMERIC(6,2),
                t2m_min NUMERIC(6,2),
                t2m_max NUMERIC(6,2),
                source_file_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (time, id)
            );
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_era5_daily_temp_time ON bronze.era5_daily_temp_stats (time);")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_era5_daily_temp_location "
            "ON bronze.era5_daily_temp_stats (latitude, longitude);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_era5_daily_temp_source_file "
            "ON bronze.era5_daily_temp_stats (source_file_id);"
        )

    # Commit table creation IMMEDIATELY, avant la partie TimescaleDB optionnelle
    conn.commit()

    with conn.cursor() as cur:
        try:
            cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")
            if cur.fetchone():
                cur.execute("""
                    SELECT create_hypertable(
                        'bronze.era5_daily_temp_stats',
                        'time',
                        chunk_time_interval => INTERVAL '1 year',
                        if_not_exists => TRUE,
                        migrate_data => TRUE
                    );
                """)
                cur.execute("""
                    ALTER TABLE bronze.era5_daily_temp_stats SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'source_file_id'
                    );
                """)
                cur.execute("""
                    SELECT add_compression_policy(
                        'bronze.era5_daily_temp_stats',
                        INTERVAL '30 days',
                        if_not_exists => TRUE
                    );
                """)
        except Exception as e:
            logging.getLogger(__name__).warning(
                "TimescaleDB setup skipped for era5_daily_temp_stats (non-blocking): %s", e
            )

        conn.commit()


def _insert_dataframe(conn, df: pd.DataFrame, context, batch_size: int = 10000) -> int:
    """Insert DataFrame into bronze.era5_daily_temp_stats in batches."""
    context.log.info(f"Inserting {len(df):,} rows in batches of {batch_size:,}...")

    total_rows = len(df)
    rows_inserted = 0

    with conn.cursor() as cur:
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i + batch_size]

            values = [
                (
                    row.time,
                    row.latitude,
                    row.longitude,
                    row.t2m_mean,
                    row.t2m_min,
                    row.t2m_max,
                    row.source_file_id,
                )
                for row in batch.itertuples(index=False)
            ]

            execute_values(
                cur,
                """
                INSERT INTO bronze.era5_daily_temp_stats
                (time, latitude, longitude, t2m_mean, t2m_min, t2m_max, source_file_id)
                VALUES %s
                """,
                values,
            )
            rows_inserted += len(batch)
            if rows_inserted % 100000 == 0 or rows_inserted == total_rows:
                context.log.info(
                    f"  {rows_inserted:,}/{total_rows:,} rows inserted ({rows_inserted / total_rows * 100:.1f}%)"
                )

    conn.commit()
    context.log.info(f"All {rows_inserted:,} rows committed to database")
    return rows_inserted


def _download_one_statistic(
    context: AssetExecutionContext,
    client: "cdsapi.Client",
    dataset: str,
    request: dict,
    stat_label: str,
    max_retries: int,
    retry_delay: float,
) -> str:
    """
    Télécharge une statistique journalière (mean/min/max) et retourne le
    chemin du fichier NetCDF local. Retry exponentiel comme era5_assets.py.
    """
    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        tmp_path = tmp.name

    delay = retry_delay
    try:
        for attempt in range(max_retries):
            try:
                client.retrieve(dataset, request, tmp_path)
                context.log.info(f"Download complete ({stat_label}): {tmp_path}")
                return tmp_path
            except Exception as e:
                if attempt < max_retries - 1:
                    context.log.warning(
                        f"CDS API failed for {stat_label} (attempt {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {delay}s..."
                    )
                    import time
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                else:
                    context.log.error(f"CDS API failed for {stat_label} after {max_retries} attempts: {e}")
                    raise
    except Exception:
        # Retries épuisés : le fichier temp vide/partiel n'a pas de propriétaire
        # (le chemin n'est jamais retourné à l'appelant) -> le nettoyer ici.
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


def _load_statistic_dataframe(
    context: AssetExecutionContext,
    nc_path: str,
    start_date: datetime,
    end_date: datetime,
    target_column: str,
) -> pd.DataFrame:
    """
    Ouvre le NetCDF d'une statistique, filtre la fenêtre exacte, convertit
    K->degC et renvoie un DataFrame [time, latitude, longitude, <target_column>].
    """
    actual_nc_path = nc_path
    ds = None
    try:
        # Défensif : le produit sert du NetCDF direct (spike §1/§6), mais on
        # garde le fallback ZIP par sécurité (comme process_era5_range_to_timeseries).
        with open(nc_path, "rb") as f:
            header = f.read(4)

        if header == b"PK\x03\x04":
            context.log.info(f"Format détecté: ZIP pour {target_column}. Extraction en cours...")
            with zipfile.ZipFile(nc_path, "r") as zf:
                nc_files = [n for n in zf.namelist() if n.endswith(".nc")]
                if not nc_files:
                    raise ValueError("Aucun fichier .nc trouvé dans le ZIP CDS.")
                target_file = nc_files[0]
                zf.extract(target_file, os.path.dirname(nc_path))
                actual_nc_path = os.path.join(os.path.dirname(nc_path), target_file)

        ds = xr.open_dataset(actual_nc_path, engine="h5netcdf")

        if "t2m" not in ds.data_vars:
            raise ValueError(f"ERA5 daily stats NetCDF missing 't2m'. Available: {list(ds.data_vars)}")

        # Coordonnée scalaire `number` (ensemble member id) à dropper (spike §2/§6)
        if "number" in ds.coords or "number" in ds.variables:
            ds = ds.drop_vars("number")

        time_dim = "valid_time" if "valid_time" in ds.dims else "time"
        if ds.dims.get(time_dim, 0) == 0:
            raise ValueError("ERA5 daily stats NetCDF has 0 time steps - corrupted or empty download")

        ds = ds.sel({time_dim: slice(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))})

        df = ds.to_dataframe().reset_index()
        df = df.rename(columns={"t2m": target_column, time_dim: "time"})
        if "number" in df.columns:
            df = df.drop(columns=["number"])

        # K -> degC, arrondi 2 décimales (table NUMERIC(6,2))
        df[target_column] = (df[target_column] - 273.15).round(2)

        # Arrondi défensif des coordonnées (pattern du mart grille ERA5, commit
        # ea9ad30) : la grille est déjà des multiples exacts de 0.1 mais le
        # merge des 3 statistiques sur (time, latitude, longitude) doit
        # comparer des floats identiques.
        df["latitude"] = df["latitude"].round(3)
        df["longitude"] = df["longitude"].round(3)

        return df[["time", "latitude", "longitude", target_column]]
    finally:
        if ds is not None:
            ds.close()
        if actual_nc_path != nc_path and os.path.exists(actual_nc_path):
            os.remove(actual_nc_path)


def process_daily_stats_range(
    context: AssetExecutionContext,
    start_date: datetime,
    end_date: datetime,
    file_id: str,
) -> int:
    """
    Pour la fenêtre [start_date, end_date] :
    1. Une requête CDS par daily_statistic (mean/min/max) -> 3 NetCDF.
    2. Chaque NetCDF -> DataFrame [time, latitude, longitude, t2m_<stat>].
    3. Fusion OUTER des 3 DataFrames sur (time, latitude, longitude).
       Choix : OUTER (pas INNER) pour ne pas perdre une cellule/jour si UNE
       des 3 statistiques manque un jour ponctuel (retard partiel côté CADS) ;
       on ne garde ensuite que les lignes où t2m_mean est renseigné (la
       moyenne est la statistique de référence). Ce filtre élimine aussi les
       cellules mer (NaN sur les 3 stats, même masque terre/mer partout).
    4. K -> degC (fait par statistique avant fusion), DELETE overlap + INSERT.
    Retourne le nombre de lignes insérées.
    """
    conn = None
    tmp_paths = []

    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST", "postgres"),
            port=os.getenv("PG_PORT", "5432"),
            database=os.getenv("PG_DB", "postgres"),
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PASSWORD"),
            sslmode=os.getenv("PG_SSLMODE", "prefer"),
        )

        _ensure_table(conn)

        context.log.info(f"Downloading ERA5 daily temp stats for {file_id}...")

        with open(CONFIG_PATH) as f:
            config = yaml.safe_load(f)

        cds_api_key = config["credentials"].get("cds_api_key")
        if not cds_api_key:
            cds_api_key_env = config["credentials"].get("cds_api_key_env", "COPERNICUS_API_KEY")
            cds_api_key = os.getenv(cds_api_key_env)

        if not cds_api_key:
            raise ValueError(
                "CDS API Key Missing! Please set the 'COPERNICUS_API_KEY' environment variable "
                "or configure 'cds_api_key' in configs/era5/era5_daily_temp_stats.yml."
            )

        client = cdsapi.Client(url=config["credentials"]["cds_api_url"], key=cds_api_key)
        dataset = config["resource"]["dataset"]

        # Dériver year/month/day de la fenêtre RÉELLE (pas un day=[1..31] figé)
        # pour éviter le cache CADS périmé — même piège que era5_assets.py.
        window_dates = []
        d = start_date
        while d <= end_date:
            window_dates.append(d)
            d += timedelta(days=1)
        years = sorted({dd.year for dd in window_dates})
        months = sorted({dd.month for dd in window_dates})
        days_list = sorted({dd.day for dd in window_dates})

        base_request = {
            "variable": [config["resource"]["variable"]],
            "year": [str(y) for y in years],
            "month": [f"{m:02d}" for m in months],
            "day": [f"{dd:02d}" for dd in days_list],
            "time_zone": config["resource"]["time_zone"],
            "frequency": config["resource"]["frequency"],
            "area": config["resource"]["area"],
        }

        max_retries = int(config.get("performance", {}).get("retry_times", 3))
        retry_delay = float(config.get("performance", {}).get("retry_delay", 10.0))

        stat_to_column = {
            "daily_mean": "t2m_mean",
            "daily_minimum": "t2m_min",
            "daily_maximum": "t2m_max",
        }

        stat_dataframes = []
        for statistic in config["resource"]["daily_statistics"]:
            target_column = stat_to_column[statistic]
            request = dict(base_request, daily_statistic=statistic)

            tmp_path = _download_one_statistic(
                context, client, dataset, request, statistic, max_retries, retry_delay
            )
            tmp_paths.append(tmp_path)

            df_stat = _load_statistic_dataframe(context, tmp_path, start_date, end_date, target_column)
            context.log.info(f"{statistic}: {len(df_stat):,} rows (raw, before merge/filter)")
            stat_dataframes.append(df_stat)

        merge_keys = ["time", "latitude", "longitude"]
        merged = stat_dataframes[0]
        for df_stat in stat_dataframes[1:]:
            merged = merged.merge(df_stat, on=merge_keys, how="outer")

        # Ne garder que les lignes où la moyenne est renseignée (cf. docstring)
        merged = merged[merged["t2m_mean"].notna()]

        merged["source_file_id"] = file_id
        merged = merged[["time", "latitude", "longitude", "t2m_mean", "t2m_min", "t2m_max", "source_file_id"]]

        # NaN -> None : psycopg2 adapte un float NaN en 'NaN'::numeric (valeur
        # acceptée par PostgreSQL et triée AU-DESSUS de toutes les autres) au
        # lieu de NULL. t2m_min/t2m_max peuvent rester NaN après le merge OUTER
        # (une statistique ponctuellement absente) : on force explicitement NULL.
        merged = merged.astype(object).where(merged.notna(), None)

        context.log.info(f"DataFrame fusionné prêt: {len(merged):,} rows")

        with conn.cursor() as cur:
            context.log.info(f"Clearing existing data for range {start_date} -> {end_date} to prevent duplicates...")
            cur.execute(
                """
                DELETE FROM bronze.era5_daily_temp_stats
                WHERE time >= %s AND time <= %s
                """,
                (start_date, end_date),
            )
            conn.commit()
            context.log.info(f"   (Deleted {cur.rowcount} overlapping rows)")

        rows_inserted = _insert_dataframe(conn, merged, context)
        return rows_inserted

    finally:
        if conn:
            conn.close()
        for tmp_path in tmp_paths:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
        gc.collect()


# ============================================================================
# ASSETS
# ============================================================================

@asset(
    compute_kind="era5",
    group_name="era5_historical",
    io_manager_key="noop_io_manager",
    partitions_def=ERA5_DAILY_TEMP_PARTITIONS_DEF,
)
def era5_daily_temp_stats_historical(context: AssetExecutionContext):
    """
    Historique ERA5 daily temp stats (1950-Present).

    Partitionné par chunks de 1 an (ERA5_DAILY_TEMP_YEARS_PER_CHUNK).
    Télécharge (3 requêtes CDS : mean/min/max) et insère directement dans
    `bronze.era5_daily_temp_stats`.
    """
    partition_key = context.partition_key
    start_year, end_year = map(int, partition_key.split("_"))

    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    file_id = f"era5_daily_temp_hist_{start_year}_{end_year}"

    context.log.info(f"Traitement historique daily temp stats: {start_year}-{end_year}")

    rows = process_daily_stats_range(context, start_date, end_date, file_id)

    return Output(
        {"rows_inserted": rows, "partition": partition_key},
        metadata={"rows": MetadataValue.int(rows)},
    )


@asset(
    compute_kind="era5",
    group_name="era5_daily_stats",
    io_manager_key="noop_io_manager",
    deps=["era5_daily_temp_stats_historical"],
)
def era5_daily_temp_stats_update(context: AssetExecutionContext):
    """
    Mise à jour quotidienne ERA5 daily temp stats - Smart Update.

    Même logique de fenêtre que `era5_weekly_update` (era5_assets.py) :
    1. MAX(time) en base - 2 jours de tampon (chevauchement volontaire pour
       robustesse, dédupliqué par le DELETE overlap + INSERT).
    2. Cap de sécurité à 60 jours de lookback (évite un timeout CDS si trou).
    3. Table vide -> fenêtre par défaut = les `max_lookback_days` derniers jours.

    Lag de disponibilité CDS : `ERA5_DAILY_STATS_LAG_DAYS` (env var) > yaml
    `extraction.availability_lag_days` > défaut 7 jours (production CDS
    observée ~6j, +1j de marge).
    """
    with open(CONFIG_PATH) as f:
        _config = yaml.safe_load(f)

    # Précédence : env var > yaml (extraction.availability_lag_days) > défaut 7j
    lag_days = int(
        os.getenv("ERA5_DAILY_STATS_LAG_DAYS", _config.get("extraction", {}).get("availability_lag_days", 7))
    )
    end_date = datetime.now() - timedelta(days=lag_days)

    last_date_in_db = None
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"),
        port=os.getenv("PG_PORT", "5432"),
        database=os.getenv("PG_DB", "postgres"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD"),
        sslmode=os.getenv("PG_SSLMODE", "prefer"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                   SELECT 1 FROM information_schema.tables
                   WHERE table_schema = 'bronze' AND table_name = 'era5_daily_temp_stats'
                )
            """)
            if cur.fetchone()[0]:
                cur.execute("SELECT MAX(time) FROM bronze.era5_daily_temp_stats")
                res = cur.fetchone()
                if res and res[0]:
                    last_date_in_db = res[0]
                    context.log.info(f"Dernière donnée en base: {last_date_in_db}")
    finally:
        conn.close()

    max_lookback_days = 60
    safety_start_date = end_date - timedelta(days=max_lookback_days)

    if last_date_in_db:
        proposed_start_date = last_date_in_db - timedelta(days=2)

        if proposed_start_date < safety_start_date:
            context.log.warning(
                f"GAP DÉTECTÉ: la base date de {last_date_in_db}, trop vieux pour l'update quotidien "
                f"(> {max_lookback_days} jours). Chargement limité aux {max_lookback_days} derniers jours. "
                "Lancer un BACKFILL MANUEL (job historique) pour combler le trou."
            )
            start_date = safety_start_date
        else:
            start_date = proposed_start_date
            context.log.info(f"Smart Update activé: reprise à {start_date.date()}")
    else:
        start_date = safety_start_date
        context.log.info(f"Table vide: initialisation avec les {max_lookback_days} derniers jours.")

    if start_date >= end_date:
        context.log.info("Données déjà à jour (Start >= End). Rien à faire.")
        return Output(
            {"rows_inserted": 0, "status": "up_to_date"},
            metadata={"status": MetadataValue.text("up_to_date")},
        )

    file_id = f"era5_daily_temp_update_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"

    context.log.info(f"Traitement daily temp stats update: {start_date.date()} -> {end_date.date()}")

    rows = process_daily_stats_range(context, start_date, end_date, file_id)

    return Output(
        {"rows_inserted": rows, "mode": "daily_smart"},
        metadata={"rows": MetadataValue.int(rows)},
    )
