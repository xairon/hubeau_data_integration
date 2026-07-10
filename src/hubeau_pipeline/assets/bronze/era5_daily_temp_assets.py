"""
ERA5 Daily Temperature Stats Bronze Layer Assets (archive horaire brute agrégée localement)

Miroir de `era5_assets.py` (client CADS, retry/backoff, DELETE overlap + INSERT
idempotent, hypertable, conversion K->degC) pour produire moyenne/min/max
journalières de `2m_temperature` dans `bronze.era5_daily_temp_stats`.

## Pourquoi l'archive horaire brute plutôt que le produit dérivé CADS

La première implémentation interrogeait `derived-era5-land-daily-statistics`
(3 requêtes CDS par fenêtre, une par statistique daily_mean/minimum/maximum,
calculées côté CADS). Ce service post-traité est une file d'attente minuscule
et globalement saturée : **~43h d'attente par ANNÉE demandée** (mesuré). Un
backfill complet 1950-présent y aurait pris ~6 semaines — inutilisable.

L'archive `reanalysis-era5-land` (horaire brute) est rapide (mesuré : 3 jours
d'horaire = 27s ; 1 MOIS complet d'horaire = 179s, 17.7 MB, ACCEPTED). Or la
donnée horaire brute EST la donnée sur laquelle le CADS calcule ces mêmes
statistiques journalières côté serveur — il suffit de refaire l'agrégation en
local (`aggregate_hourly_to_daily`, groupby+mean/min/max) pour obtenir un
résultat équivalent.

**Équivalence vérifiée empiriquement** (2026-07) en comparant, cellule-jour
par cellule-jour, l'agrégation locale de l'horaire brut contre les données
1950 déjà en base via le produit dérivé (34 488 cellule-jours) :
- Tn (minimum) et Tx (maximum) : identiques à 0.0000°C près (100% des lignes)
- moyenne : identique à 0.01°C près (arrondi float de la colonne NUMERIC(6,2)
  côté PostgreSQL, pas un écart réel de calcul)

Conclusion : même donnée, ~4h de traitement au lieu de 6 semaines.

## Design

1 requête CDS par MOIS calendaire (dataset `reanalysis-era5-land`, variable
`2m_temperature`, 24 pas horaires, jours 1-31, zone France) au lieu de 3
requêtes par fenêtre pour le produit dérivé. Les mois d'une partition sont
téléchargés CONCURREMMENT (`ThreadPoolExecutor`, un `cdsapi.Client` par
thread — cf. `_build_cds_client`) car l'archive brute n'est pas saturée et
absorbe plusieurs requêtes en parallèle sans dégrader le temps de traitement.
Chaque mois est agrégé en DataFrame journalier (`aggregate_hourly_to_daily`),
puis tous les mois sont concaténés et filtrés EXACTEMENT sur la fenêtre
demandée (utile pour `era5_daily_temp_stats_update`, dont la fenêtre ne
coïncide pas forcément avec des bornes de mois).

Entrées normatives :
- Spec design : docs/superpowers/specs/2026-07-07-era5-daily-temperature-stats-design.md
- Spike CDS   : .superpowers/sdd/spike-cds-daily-stats.md (structure NetCDF,
  gotchas `number`/`valid_time`/Kelvin/NaN=mer — toujours valables pour
  l'horaire brut, même structure de fichier)
- Référence   : src/hubeau_pipeline/assets/bronze/era5_assets.py

Câblé dans assets/__init__.py, jobs/era5_jobs.py (era5_daily_temp_historical_load
partitionné + era5_daily_temp_update_job) et schedules.py (03h30 UTC).

⚠️ Ops : lancer les matérialisations via l'UI/GraphQL (run queue, max_concurrent_runs=1),
PAS via `docker exec ... dagster asset materialize` — un client CLI tué laisse un step
zombie qui continue d'insérer (doublons constatés le 2026-07-07, dédoublonnés).
"""

import gc
import logging
import os
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
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
# SHARED HELPERS - DB
# ============================================================================

def _connect():
    """
    Connexion PostgreSQL COURTE DURÉE, à utiliser uniquement pour du travail DB
    bref (ensure_table, lecture MAX(time), DELETE+INSERT post-fusion).

    NE JAMAIS garder l'objet retourné ouvert pendant la phase de téléchargement
    CDS : celle-ci peut attendre plusieurs HEURES en file CADS, et PostgreSQL
    tue les connexions inactives trop longtemps -> `server closed the
    connection unexpectedly` (run 1950 du 2026-07-07, échec après 6h34
    d'attente, données téléchargées perdues faute de connexion valide pour
    les insérer).
    """
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"),
        port=os.getenv("PG_PORT", "5432"),
        database=os.getenv("PG_DB", "postgres"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD"),
        sslmode=os.getenv("PG_SSLMODE", "prefer"),
    )


def _ensure_table(conn):
    """Create bronze.era5_daily_temp_stats if not exists (idempotent). Schéma inchangé."""
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


def _delete_overlap_and_insert(
    context: AssetExecutionContext,
    start_date: datetime,
    end_date: datetime,
    merged: pd.DataFrame,
) -> int:
    """DELETE overlap + INSERT sur UNE connexion fraîche (ouverte et fermée ici)."""
    conn = _connect()
    try:
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

        return _insert_dataframe(conn, merged, context)
    finally:
        conn.close()


def _delete_overlap_and_insert_with_retry(
    context: AssetExecutionContext,
    start_date: datetime,
    end_date: datetime,
    merged: pd.DataFrame,
    max_attempts: int = 2,
) -> int:
    """
    DELETE overlap + INSERT sur une connexion FRAÎCHE, ouverte APRÈS la fin des
    téléchargements CDS (jamais tenue pendant l'attente en file, cf. `_connect`).

    Retry UNE fois (max_attempts=2) sur erreur DB transitoire
    (OperationalError/DatabaseError), avec une NOUVELLE connexion. Les
    téléchargements CDS sont la partie coûteuse : on ne veut pas les perdre
    pour un hoquet DB passager après coup.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return _delete_overlap_and_insert(context, start_date, end_date, merged)
        except (psycopg2.OperationalError, psycopg2.DatabaseError) as e:
            if attempt < max_attempts:
                context.log.warning(
                    f"DB error during DELETE+INSERT (attempt {attempt}/{max_attempts}): {e}. "
                    "Retrying once with a fresh connection (downloaded data preserved)..."
                )
            else:
                context.log.error(f"DB error during DELETE+INSERT after {max_attempts} attempts: {e}")
                raise


# ============================================================================
# SHARED HELPERS - CDS download (archive horaire brute, 1 requête / mois)
# ============================================================================

def _build_cds_client(cds_api_url: str, cds_api_key: str) -> "cdsapi.Client":
    """Instancie un client CDS dédié (un par thread : cdsapi.Client n'est pas garanti thread-safe si partagé)."""
    return cdsapi.Client(url=cds_api_url, key=cds_api_key)


def _months_in_range(start_date: datetime, end_date: datetime) -> list[tuple[int, int]]:
    """Liste des mois calendaires (year, month) couvrant [start_date, end_date], bornes incluses."""
    months = []
    year, month = start_date.year, start_date.month
    while (year, month) <= (end_date.year, end_date.month):
        months.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months


def _build_month_request(config: dict, year: int, month: int) -> dict:
    """
    Requête CDS `reanalysis-era5-land` pour UN mois calendaire complet (jours
    1-31 fixes, 24 pas horaires) — l'agrégation locale filtre ensuite sur la
    fenêtre exacte demandée. Contrairement à l'ancienne ingestion horaire
    (era5_assets.py), pas besoin de dériver les jours réels de la fenêtre : ce
    n'est pas un produit "dérivé" sujet à un cache CADS périmé, la donnée
    brute d'un mois passé est stable.
    """
    return {
        "variable": [config["resource"]["variable"]],
        "year": str(year),
        "month": f"{month:02d}",
        "day": [f"{d:02d}" for d in range(1, 32)],
        "time": list(config["resource"]["hours"]),
        "area": config["resource"]["area"],
        "data_format": "netcdf",
    }


def _download_cds_request(
    context: AssetExecutionContext,
    client: "cdsapi.Client",
    dataset: str,
    request: dict,
    label: str,
    max_retries: int,
    retry_delay: float,
) -> str:
    """
    Télécharge une requête CDS (1 mois d'horaire brut) et retourne le chemin
    du fichier local (NetCDF direct ou ZIP contenant un NetCDF). Retry
    exponentiel comme era5_assets.py.
    """
    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        tmp_path = tmp.name

    delay = retry_delay
    try:
        for attempt in range(max_retries):
            try:
                client.retrieve(dataset, request, tmp_path)
                context.log.info(f"Download complete ({label}): {tmp_path}")
                return tmp_path
            except Exception as e:
                if attempt < max_retries - 1:
                    context.log.warning(
                        f"CDS API failed for {label} (attempt {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {delay}s..."
                    )
                    import time
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                else:
                    context.log.error(f"CDS API failed for {label} after {max_retries} attempts: {e}")
                    raise
    except Exception:
        # Retries épuisés : le fichier temp vide/partiel n'a pas de propriétaire
        # (le chemin n'est jamais retourné à l'appelant) -> le nettoyer ici.
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


# ============================================================================
# CORE AGGREGATION (pure, testable sans I/O réseau/DB)
# ============================================================================

def aggregate_hourly_to_daily(ds: "xr.Dataset") -> pd.DataFrame:
    """
    Agrège un Dataset xarray HORAIRE ERA5-Land (variable `t2m`, Kelvin) en
    moyenne/min/max JOURNALIÈRES par cellule de grille, en °C.

    Remplace les 3 requêtes CDS `derived-era5-land-daily-statistics` (service
    saturé, ~43h/an) : la donnée horaire brute EST la donnée sur laquelle le
    CADS calcule ces mêmes statistiques -> on refait l'agrégation en local.
    Équivalence vérifiée empiriquement (2026-07) contre les données 1950 déjà
    en base : Tn/Tx identiques à 0.0000°C (100%), moyenne à 0.01°C près
    (arrondi float de la colonne NUMERIC(6,2)).

    Étapes (design figé) :
    1. Dim temporelle `valid_time` (fallback `time`), variable `t2m`.
    2. Coordonnée scalaire `number` (ensemble member id) droppée si présente.
    3. Kelvin -> °C.
    4. groupby(f"{tdim}.date") -> .mean() / .min() / .max().
    5. Arrondi grille 0.1° (lat/lon) + arrondi 2 décimales (temps, colonne
       NUMERIC(6,2)).
    6. Lignes où t2m_mean est NaN supprimées (cellules mer : masque terre/mer
       identique sur les 3 statistiques, donc aussi filtre implicite sur
       t2m_min/t2m_max).

    Retourne un DataFrame [time, latitude, longitude, t2m_mean, t2m_min, t2m_max].
    NaN résiduel (ex: t2m_min/max NaN alors que t2m_mean ne l'est pas — cas
    limite non attendu en pratique) est laissé tel quel ; la conversion
    NaN -> None pour l'insertion DB est faite par l'appelant.
    """
    if "number" in ds.coords or "number" in ds.variables:
        ds = ds.drop_vars("number")

    if "t2m" not in ds.data_vars:
        raise ValueError(f"ERA5-Land hourly Dataset missing 't2m'. Available: {list(ds.data_vars)}")

    tdim = "valid_time" if "valid_time" in ds.dims else "time"
    if ds.sizes.get(tdim, 0) == 0:
        raise ValueError("ERA5-Land hourly Dataset has 0 time steps - corrupted or empty download")

    t2m_celsius = ds["t2m"] - 273.15  # K -> degC, avant agrégation

    grouped = t2m_celsius.groupby(f"{tdim}.date")
    daily = xr.Dataset(
        {
            "t2m_mean": grouped.mean(),
            "t2m_min": grouped.min(),
            "t2m_max": grouped.max(),
        }
    )

    df = daily.to_dataframe().reset_index()
    df = df.rename(columns={"date": "time"})
    df["time"] = pd.to_datetime(df["time"])

    # Arrondi grille 0.1° (ERA5-Land natif) : évite les artefacts float lors
    # des concaténations/filtrages sur latitude/longitude entre mois.
    df["latitude"] = df["latitude"].round(1)
    df["longitude"] = df["longitude"].round(1)
    for col in ("t2m_mean", "t2m_min", "t2m_max"):
        df[col] = df[col].round(2)

    # Cellules mer : t2m_mean NaN sur toute la fenêtre -> supprimées.
    df = df[df["t2m_mean"].notna()]

    return df[["time", "latitude", "longitude", "t2m_mean", "t2m_min", "t2m_max"]]


def _load_month_daily_dataframe(context: AssetExecutionContext, nc_path: str, year: int, month: int) -> pd.DataFrame:
    """
    Ouvre le fichier téléchargé pour un mois (NetCDF direct ou ZIP contenant
    un NetCDF, mirroir de era5_assets.py), agrège via
    `aggregate_hourly_to_daily`, et nettoie les fichiers intermédiaires.
    """
    ds = None
    actual_nc_path = nc_path
    try:
        with open(nc_path, "rb") as f:
            header = f.read(4)

        if header == b"PK\x03\x04":
            context.log.info(f"Format détecté: ZIP pour {year}-{month:02d}. Extraction en cours...")
            with zipfile.ZipFile(nc_path, "r") as zf:
                nc_files = [n for n in zf.namelist() if n.endswith(".nc")]
                if not nc_files:
                    raise ValueError("Aucun fichier .nc trouvé dans le ZIP CDS.")
                target_file = nc_files[0]
                zf.extract(target_file, os.path.dirname(nc_path))
                actual_nc_path = os.path.join(os.path.dirname(nc_path), target_file)

        ds = xr.open_dataset(actual_nc_path, engine="h5netcdf")

        df = aggregate_hourly_to_daily(ds)
        context.log.info(f"{year}-{month:02d}: {len(df):,} lignes agrégées (jour x cellule, mer exclue)")
        return df
    finally:
        if ds is not None:
            ds.close()
        if actual_nc_path != nc_path and os.path.exists(actual_nc_path):
            os.remove(actual_nc_path)


# ============================================================================
# ORCHESTRATION - download concurrent des mois + agrégation + DELETE/INSERT
# ============================================================================

def process_daily_stats_range(
    context: AssetExecutionContext,
    start_date: datetime,
    end_date: datetime,
    file_id: str,
) -> int:
    """
    Pour la fenêtre [start_date, end_date] :
    1. Détermine les mois calendaires à télécharger (`_months_in_range`).
    2. Une requête CDS `reanalysis-era5-land` PAR MOIS (horaire brut, 24 pas,
       jours 1-31), soumises CONCURREMMENT (ThreadPoolExecutor, un client CDS
       par thread, `months_concurrency` dans le yaml — défaut 4). L'archive
       brute n'est pas saturée comme le produit dérivé : la parallélisation
       accélère une partition multi-mois sans dégrader le service.
    3. Chaque mois -> DataFrame journalier via `aggregate_hourly_to_daily`
       (agrégation LOCALE mean/min/max, remplace les 3 requêtes CDS du
       produit dérivé).
    4. Concaténation de tous les mois, puis filtre sur la fenêtre EXACTE
       demandée (utile pour `era5_daily_temp_stats_update`, dont la fenêtre
       ne coïncide pas forcément avec des bornes de mois calendaires).
    5. DELETE overlap + INSERT (idempotent, inchangé).
    Retourne le nombre de lignes insérées.

    Cycle de vie des connexions DB : AUCUNE connexion n'est tenue ouverte
    pendant la phase de téléchargement CDS (étape 2). `_ensure_table` utilise
    une connexion courte fermée avant les téléchargements ; le DELETE+INSERT
    (étape 5) ouvre une connexion FRAÎCHE après leur fin. Raison : connexions
    tuées côté PostgreSQL après une attente trop longue -> perte des données
    déjà téléchargées (run 1950 du 2026-07-07, échec après 6h34 d'attente,
    avant le passage à l'archive horaire brute).
    """
    tmp_paths = []

    try:
        # Connexion COURTE DURÉE : fermée avant le début des téléchargements CDS.
        ensure_conn = _connect()
        try:
            _ensure_table(ensure_conn)
        finally:
            ensure_conn.close()

        context.log.info(f"Downloading ERA5-Land hourly raw data for {file_id}...")

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

        cds_api_url = config["credentials"]["cds_api_url"]
        dataset = config["resource"]["dataset"]

        max_retries = int(config.get("performance", {}).get("retry_times", 3))
        retry_delay = float(config.get("performance", {}).get("retry_delay", 10.0))
        months_concurrency = int(config.get("performance", {}).get("months_concurrency", 4))

        month_keys = _months_in_range(start_date, end_date)
        context.log.info(
            f"Fenêtre {start_date.date()} -> {end_date.date()} : {len(month_keys)} mois à télécharger "
            f"(concurrence={months_concurrency})"
        )

        def _download_for_month(year: int, month: int) -> tuple[tuple[int, int], str]:
            # Client CDS dédié à ce thread (cf. _build_cds_client).
            thread_client = _build_cds_client(cds_api_url, cds_api_key)
            request = _build_month_request(config, year, month)
            label = f"{year}-{month:02d}"
            tmp_path = _download_cds_request(context, thread_client, dataset, request, label, max_retries, retry_delay)
            return (year, month), tmp_path

        # Soumission concurrente des mois : `as_completed` attend TOUTES les
        # tâches avant de sortir la boucle, donc un échec sur un thread
        # n'interrompt pas les autres (ils ont le temps de finir avant qu'on
        # ne lève l'exception agrégée ci-dessous). Succès -> fichier temp
        # conservé pour cleanup global ; échec -> `_download_cds_request`
        # nettoie déjà son propre fichier temp partiel.
        download_results: dict[tuple[int, int], str] = {}
        errors: list[tuple[tuple[int, int], Exception]] = []

        with ThreadPoolExecutor(max_workers=months_concurrency) as executor:
            futures = {executor.submit(_download_for_month, y, m): (y, m) for (y, m) in month_keys}
            for future in as_completed(futures):
                key = futures[future]
                try:
                    result_key, tmp_path = future.result()
                    download_results[result_key] = tmp_path
                    tmp_paths.append(tmp_path)
                except Exception as e:
                    errors.append((key, e))

        if errors:
            failed_months = ", ".join(f"{y}-{m:02d}" for (y, m), _ in errors)
            context.log.error(f"Téléchargement échoué pour: {failed_months}")
            # tmp_paths des téléchargements réussis sont nettoyés par le
            # `finally` de cette fonction ; on relève la 1ère erreur rencontrée.
            raise errors[0][1]

        month_dataframes = []
        for year, month in month_keys:
            tmp_path = download_results[(year, month)]
            df_month = _load_month_daily_dataframe(context, tmp_path, year, month)
            month_dataframes.append(df_month)

        if month_dataframes:
            merged = pd.concat(month_dataframes, ignore_index=True)
        else:
            merged = pd.DataFrame(columns=["time", "latitude", "longitude", "t2m_mean", "t2m_min", "t2m_max"])

        # Filtre EXACT sur la fenêtre demandée (les requêtes mensuelles
        # couvrent le mois entier, la fenêtre update peut être plus étroite).
        window_start = pd.Timestamp(start_date.date())
        window_end = pd.Timestamp(end_date.date())
        merged = merged[(merged["time"] >= window_start) & (merged["time"] <= window_end)]

        merged["source_file_id"] = file_id
        merged = merged[["time", "latitude", "longitude", "t2m_mean", "t2m_min", "t2m_max", "source_file_id"]]

        # NaN -> None : psycopg2 adapte un float NaN en 'NaN'::numeric (valeur
        # acceptée par PostgreSQL et triée AU-DESSUS de toutes les autres) au
        # lieu de NULL.
        merged = merged.astype(object).where(merged.notna(), None)

        context.log.info(f"DataFrame fusionné prêt: {len(merged):,} rows")

        # Connexion FRAÎCHE ouverte APRÈS la fin des téléchargements (cf.
        # docstring). Retry 1x sur erreur DB transitoire pour ne jamais perdre
        # des téléchargements CDS pour un hoquet DB passager.
        return _delete_overlap_and_insert_with_retry(context, start_date, end_date, merged)

    finally:
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
    Télécharge l'archive horaire brute (12 requêtes CDS/an, une par mois,
    concurrentes) et agrège localement mean/min/max avant insertion directe
    dans `bronze.era5_daily_temp_stats`.
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

    # Connexion COURTE DURÉE : uniquement pour lire MAX(time), fermée dans le
    # `finally` ci-dessous AVANT l'appel à `process_daily_stats_range` (qui
    # gère lui-même son propre cycle de connexions autour des téléchargements
    # CDS — cf. docstring de `_connect`).
    last_date_in_db = None
    conn = _connect()
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
