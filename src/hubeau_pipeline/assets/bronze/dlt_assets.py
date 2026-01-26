"""
Bronze Layer DLT Assets - Hub'Eau Data Pipeline

Uses official dagster-dlt integration (@dlt_assets decorator).
DLT handles schema inference and data loading to PostgreSQL.

DOMAINS:
- Piezometry (stations + chroniques)
- Hydrometry (sites + stations + observations)
"""

import os
import yaml
import dlt
import psycopg2
from dlt.destinations import postgres
from dagster import AssetExecutionContext, StaticPartitionsDefinition, Output, MetadataValue, AssetIn
from dagster_dlt import DagsterDltResource, dlt_assets, DagsterDltTranslator
from datetime import datetime
from typing import Dict, Any, List

from hubeau_pipeline.sources.hubeau_csv_source import (
    hubeau_stations,
    hubeau_chroniques_year,
    hubeau_chroniques_daily,
)
from hubeau_pipeline.resources import PostgreSQLResource


# ============================================================================
# PARTITIONS
# ============================================================================

CURRENT_YEAR = datetime.now().year
OLDEST_YEAR = 1967
YEAR_PARTITIONS = [str(year) for year in range(OLDEST_YEAR, CURRENT_YEAR + 1)]
MODE_PARTITIONS = StaticPartitionsDefinition(YEAR_PARTITIONS)


# ============================================================================
# HELPERS
# ============================================================================

def _load_config(name: str) -> dict:
    """Load YAML configuration for a resource."""
    config_paths = [
        f"/app/configs/hubeau/{name}.yml",  # Docker
        f"configs/hubeau/{name}.yml",  # Local
    ]
    for path in config_paths:
        try:
            with open(path) as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            continue
    raise FileNotFoundError(f"Config not found for {name}")


def _create_pipeline(name: str) -> dlt.Pipeline:
    """
    Create DLT pipeline with PostgreSQL destination.
    Uses environment variables for credentials (dagster-dlt best practice).
    """
    destination = postgres(
        credentials={
            "database": os.environ.get("PG_DB", "postgres"),
            "username": os.environ.get("PG_USER", "postgres"),
            "password": os.environ.get("PG_PASSWORD"),
            "host": os.environ.get("PG_HOST", "postgres"),
            "port": int(os.environ.get("PG_PORT", "5432")),
        }
    )
    
    return dlt.pipeline(
        pipeline_name=name,
        destination=destination,
        dataset_name=os.environ.get("DLT_BRONZE_DATASET", "bronze"),
        progress="log",
    )


# ============================================================================
# DLT SOURCES
# ============================================================================

def create_referentiel_source(
    name: str,
    config: Dict[str, Any],
    resource_name: str
):
    """Factory to create a DLT source for a referentiel (stations/sites) endpoint."""
    @dlt.source(name=name)
    def _source():
        @dlt.resource(name=resource_name, write_disposition="replace", parallelized=False)
        def _resource():
            yield from hubeau_stations(config)
        return _resource
    return _source

@dlt.source(name="hubeau_piezometry_stations")
def piezometry_stations_source():
    """DLT Source for Piezometry Stations (FULL load with replace)."""
    return create_referentiel_source(
        name="hubeau_piezometry_stations",
        config=_load_config("piezometry_stations"),
        resource_name="piezometry_stations_raw"
    )()

# ============================================================================
# LAZY LOADING RESOURCE - BEST PRACTICE
# ============================================================================

def _clean_partition_data(table_name: str, schema: str, year: str, date_column: str, logger=None) -> int:
    """
    Nettoie les données existantes pour une partition (année) avant rechargement.
    
    Args:
        table_name: Nom de la table (ex: "piezometry_chroniques_raw")
        schema: Schéma de la table (ex: "bronze")
        year: Année de la partition (ex: "2020")
        date_column: Nom de la colonne de date (ex: "date_mesure")
        logger: Logger Dagster (optionnel)
    
    Returns:
        Nombre de lignes supprimées
    """
    log = logger.info if logger else print
    
    try:
        host = os.environ.get("PG_HOST", "postgres")
        db = os.environ.get("PG_DB", "postgres")
        user = os.environ.get("PG_USER", "postgres")
        password = os.environ.get("PG_PASSWORD")
        
        conn = psycopg2.connect(
            host=host,
            port=int(os.environ.get("PG_PORT", "5432")),
            database=db,
            user=user,
            password=password,
        )
        
        with conn:
            with conn.cursor() as cur:
                # Vérifier si la table existe
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 
                        FROM information_schema.tables 
                        WHERE table_schema = %s AND table_name = %s
                    )
                """, (schema, table_name))
                
                table_exists = cur.fetchone()[0]
                
                if not table_exists:
                    log(f"📋 Table {schema}.{table_name} n'existe pas encore, pas de nettoyage nécessaire")
                    return 0
                
                # Compter les lignes existantes pour cette année
                # Note: date_column peut être VARCHAR dans bronze
                # On utilise LIKE pour filtrer par année (plus robuste que EXTRACT sur VARCHAR)
                cur.execute(f"""
                    SELECT COUNT(*) 
                    FROM "{schema}"."{table_name}"
                    WHERE "{date_column}" IS NOT NULL
                      AND "{date_column}"::text LIKE %s
                """, (f"{year}-%",))
                
                existing_count = cur.fetchone()[0]
                
                if existing_count == 0:
                    log(f"📋 Aucune donnée existante pour l'année {year} dans {schema}.{table_name}")
                    return 0
                
                # Supprimer les données de cette année
                # Note: date_column peut être VARCHAR dans bronze
                # On utilise LIKE pour filtrer par année (plus robuste que EXTRACT sur VARCHAR)
                log(f"🧹 Suppression de {existing_count:,} lignes existantes pour l'année {year}...")
                cur.execute(f"""
                    DELETE FROM "{schema}"."{table_name}"
                    WHERE "{date_column}" IS NOT NULL
                      AND "{date_column}"::text LIKE %s
                """, (f"{year}-%",))
                
                deleted_count = cur.rowcount
                conn.commit()
                
                log(f"✅ {deleted_count:,} lignes supprimées pour l'année {year}")
                return deleted_count
                
    except Exception as e:
        error_msg = f"❌ Erreur lors du nettoyage de la partition {year}: {e}"
        if logger:
            logger.error(error_msg)
        else:
            print(error_msg, flush=True)
        # Ne pas bloquer le chargement si le nettoyage échoue
        return 0


def _fetch_distinct_column_values(table_name: str, column_name: str, logger=None) -> List[str]:
    """
    Helper to fetch distinct values from a column in PG at runtime.
    Uses standard env vars available in the container.
    """
    log = logger.info if logger else print
    log(f"🔍 Récupération des valeurs distinctes pour {column_name} dans {table_name}...")
    try:
        host = os.environ.get("PG_HOST", "postgres")
        db = os.environ.get("PG_DB", "postgres")
        user = os.environ.get("PG_USER", "postgres")
        
        conn = psycopg2.connect(
            host=host,
            port=int(os.environ.get("PG_PORT", "5432")),
            database=db,
            user=user,
            password=os.environ.get("PG_PASSWORD"),
        )
        with conn:
            with conn.cursor() as cur:
                schema = os.environ.get("DLT_BRONZE_DATASET", "bronze")
                query = f'SELECT DISTINCT "{column_name}" FROM "{schema}"."{table_name}" WHERE "{column_name}" IS NOT NULL'
                cur.execute(query)
                results = [str(row[0]) for row in cur.fetchall()]
                log(f"✅ {len(results):,} valeurs récupérées pour {table_name}.{column_name}")
                return results
    except Exception as e:
        error_msg = f"❌ Erreur lors de la récupération depuis PG: {e}"
        if logger:
            logger.error(error_msg)
        else:
            print(error_msg, flush=True)
        return []

def create_piezometry_chroniques_resource(year: str, dagster_context=None):
    """
    Factory that creates a resource with NO data arguments.
    The resource fetches its own data (stations) when it starts running.
    
    Note: Utilise append car il peut y avoir plusieurs chroniques légitimes
    pour la même station/date (différents mode_obtention, statut, qualification).
    Les doublons de relance sont évités via un hash des données dans _dlt_id.
    """
    config = _load_config("piezometry_chroniques")
    
    @dlt.resource(
        name="piezometry_chroniques_raw",
        write_disposition="append",  # append: doublons légitimes possibles (même station/date)
        # Pas de primary_key: plusieurs mesures légitimes par station/date possibles
        # Les doublons de relance sont gérés via _dlt_id (hash des données)
        parallelized=False
    )
    def _resource():
        # 1. LAZY LOAD: Fetch stations NOW (Runtime), inside the generator
        # This avoids passing 23k items to the decorator configuration
        # Resolve logger from context if available
        logger = dagster_context.log if dagster_context else None
        log = logger.info if logger else print

        log("🔄 Démarrage du générateur de ressource DLT...")
        
        station_codes = _fetch_distinct_column_values(
            "piezometry_stations", 
            "code_bss", 
            logger=logger
        )
        
        if not station_codes:
            warning_msg = "⚠️ Aucun code de station trouvé! Le pipeline ne produira aucune donnée."
            if logger:
                logger.warning(warning_msg)
            else:
                print(warning_msg, flush=True)
            return

        log(f"📊 {len(station_codes):,} stations à traiter pour l'année {year}")
        
        # Calculate batch info for logging
        batch_size = config.get("extraction", {}).get("station_slicing", {}).get("batch_size", 30)
        total_batches = (len(station_codes) + batch_size - 1) // batch_size
        log(f"📦 Les stations seront traitées en {total_batches} lots de {batch_size} stations")

        # 2. Delegate to the logic in source file
        yield from hubeau_chroniques_year(
            config, 
            year=year,
            station_codes=station_codes,
            dagster_context=dagster_context
        )
    
    return _resource


@dlt.source(name="hubeau_hydrometry_sites")
def hydrometry_sites_source():
    """DLT Source for Hydrometry Sites (FULL load with replace)."""
    return create_referentiel_source(
        name="hubeau_hydrometry_sites",
        config=_load_config("hydrometry_sites"),
        resource_name="hydrometry_sites_raw"
    )()


@dlt.source(name="hubeau_hydrometry_stations")
def hydrometry_stations_source():
    """DLT Source for Hydrometry Stations (FULL load with replace)."""
    return create_referentiel_source(
        name="hubeau_hydrometry_stations",
        config=_load_config("hydrometry_stations"),
        resource_name="hydrometry_stations_raw"
    )()


def create_hydrometry_obs_elab_resource(year: str, dagster_context=None):
    """
    Factory that creates a resource for Hydrometry Observations (YEAR partition).
    Matches the pattern used in create_piezometry_chroniques_resource.
    """
    config = _load_config("hydrometry_obs_elab")
    
    @dlt.resource(
        name="hydrometry_obs_elab_raw",
        write_disposition="append",  # append car doublons légitimes possibles
        parallelized=False
    )
    def _resource():
        # Lazy load station codes from hydrometry_stations_raw
        logger = dagster_context.log if dagster_context else None
        log = logger.info if logger else print
        
        log("🔄 Démarrage du générateur de ressource DLT (hydrometry obs_elab)...")
        
        station_codes = _fetch_distinct_column_values(
            "hydrometry_stations",
            "code_station",
            logger=logger
        )
        
        if not station_codes:
            warning_msg = "⚠️ Aucun code de station hydrométrique trouvé! Le pipeline ne produira aucune donnée."
            if logger:
                logger.warning(warning_msg)
            else:
                print(warning_msg, flush=True)
            return

        log(f"📊 {len(station_codes):,} stations à traiter pour l'année {year}")
        
        # Calculate batch info for logging
        batch_size = config.get("extraction", {}).get("station_slicing", {}).get("batch_size", 50)
        total_batches = (len(station_codes) + batch_size - 1) // batch_size
        log(f"📦 Les stations seront traitées en {total_batches} lots de {batch_size} stations")

        yield from hubeau_chroniques_year(
            config, 
            year=year, 
            station_codes=station_codes,
            dagster_context=dagster_context
        )
    
    return _resource


# ============================================================================
# DAGSTER ASSETS using @dlt_assets (Official Pattern)
# ============================================================================

# Piezometry Stations
@dlt_assets(
    dlt_source=piezometry_stations_source(),
    dlt_pipeline=_create_pipeline("hubeau_piezometry_stations"),
    name="piezometry_stations",
    group_name="piezometry_stations",
)
def piezometry_stations_raw(context: AssetExecutionContext, dlt: DagsterDltResource):
    """Piezometry stations - FULL load using dagster-dlt."""
    yield from dlt.run(context=context)


# Hydrometry Sites
@dlt_assets(
    dlt_source=hydrometry_sites_source(),
    dlt_pipeline=_create_pipeline("hubeau_hydrometry_sites"),
    name="hydrometry_sites",
    group_name="hydrometry_sites",
)
def hydrometry_sites_raw(context: AssetExecutionContext, dlt: DagsterDltResource):
    """Hydrometry sites - FULL load using dagster-dlt."""
    yield from dlt.run(context=context)


# Hydrometry Stations
@dlt_assets(
    dlt_source=hydrometry_stations_source(),
    dlt_pipeline=_create_pipeline("hubeau_hydrometry_stations"),
    name="hydrometry_stations",
    group_name="hydrometry_stations",
)
def hydrometry_stations_raw(context: AssetExecutionContext, dlt: DagsterDltResource):
    """Hydrometry stations - FULL load using dagster-dlt."""
    yield from dlt.run(context=context)


# ============================================================================
# PARTITIONED ASSETS (need special handling)
# ============================================================================
# Note: @dlt_assets with partitions requires DagsterDltTranslator customization.
# For now, we use regular @asset pattern for partitioned data until dagster-dlt
# supports StaticPartitionsDefinition natively.

from dagster import asset, Output, MetadataValue, AssetIn




@asset(
    compute_kind="dlt",
    group_name="piezometry_chroniques",
    partitions_def=MODE_PARTITIONS,
    deps=["piezometry_stations_raw"]
)
def piezometry_chroniques_raw(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Piezometry chroniques - Partitioned by year.
    Uses 'Lazy Loading' pattern to avoid DLT config serialization issues.
    """
    year = context.partition_key
    context.log.info(f"📅 Traitement de la partition: {year}")

    # Nettoyer les données existantes pour cette année avant rechargement
    # Cela évite les doublons de relance tout en permettant les doublons légitimes
    deleted_count = _clean_partition_data(
        table_name="piezometry_chroniques_raw",
        schema="bronze",
        year=year,
        date_column="date_mesure",
        logger=context.log
    )
    
    if deleted_count > 0:
        context.log.info(f"🧹 Nettoyage terminé: {deleted_count:,} lignes supprimées pour l'année {year}")

    # Create pipeline
    context.log.info("🔧 Création du pipeline DLT...")
    pipeline = _create_pipeline(f"hubeau_piezometry_chroniques_{year}")
    context.log.info("✅ Pipeline DLT créé")
    
    # Create resource WITHOUT large arguments
    # The resource will self-bootstrap its station list from PG
    context.log.info("🏭 Création de la ressource DLT (chargement paresseux des stations)...")
    resource = create_piezometry_chroniques_resource(year, dagster_context=context)
    context.log.info("✅ Ressource DLT créée")
    
    context.log.info("🚀 Démarrage de l'exécution du pipeline DLT...")
    import time
    start_time = time.time()
    load_info = pipeline.run(resource)
    elapsed_time = time.time() - start_time
    
    context.log.info(f"⏱️ Pipeline terminé en {elapsed_time:.1f} secondes")
    
    # DEBUG: Log full load info
    context.log.info(f"📋 DLT Load Info:\n{load_info}")
    
    # Metrics extraction
    rows = 0
    try:
        packages = getattr(load_info, "load_packages", []) or []
        for pkg in packages:
            # Check if jobs is a dict or list
            jobs = getattr(pkg, "jobs", [])
            if isinstance(jobs, dict):
                # Handle case where jobs might be grouped by status (older DLT or internal repr)
                all_jobs = []
                for job_list in jobs.values():
                    if isinstance(job_list, list):
                        all_jobs.extend(job_list)
                jobs = all_jobs

            for job in jobs:
                rows += getattr(job, "metrics", {}).get("items", 0)
    except Exception as e:
        context.log.error(f"⚠️ Failed to extract metrics from load_info: {e}")
    
    context.log.info(f"✅ Chargement terminé: {rows:,} lignes chargées pour l'année {year}")
    
    return Output(
        {"rows_loaded": rows, "year": year},
        metadata={
            "rows_loaded": MetadataValue.int(rows),
            "partition": MetadataValue.text(year),
            "execution_time_seconds": MetadataValue.float(elapsed_time),
        }
    )


@asset(
    compute_kind="dlt",
    group_name="hydrometry_chroniques",
    partitions_def=MODE_PARTITIONS,
)
def hydrometry_obs_elab_raw(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Hydrometry observations - Partitioned by year.
    Uses 'Lazy Loading' pattern to avoid DLT config serialization issues.
    """
    import time
    
    year = context.partition_key
    context.log.info(f"📅 Traitement de la partition: {year}")
    
    # Nettoyer les données existantes pour cette année avant rechargement
    deleted_count = _clean_partition_data(
        table_name="hydrometry_obs_elab_raw",
        schema="bronze",
        year=year,
        date_column="date_obs_elab",
        logger=context.log
    )
    
    if deleted_count > 0:
        context.log.info(f"🧹 Nettoyage terminé: {deleted_count:,} lignes supprimées pour l'année {year}")
    
    # Create pipeline
    context.log.info("🔧 Création du pipeline DLT...")
    pipeline = _create_pipeline(f"hubeau_hydrometry_obs_elab_{year}")
    context.log.info("✅ Pipeline DLT créé")
    
    # Create resource with lazy loading
    context.log.info("🏭 Création de la ressource DLT (chargement paresseux des stations)...")
    resource = create_hydrometry_obs_elab_resource(year, dagster_context=context)
    context.log.info("✅ Ressource DLT créée")
    
    context.log.info("🚀 Démarrage de l'exécution du pipeline DLT...")
    start_time = time.time()
    load_info = pipeline.run(resource)
    elapsed_time = time.time() - start_time
    
    context.log.info(f"⏱️ Pipeline terminé en {elapsed_time:.1f} secondes")
    
    # DEBUG: Log full load info
    context.log.info(f"📋 DLT Load Info:\n{load_info}")
    
    # Metrics extraction
    rows = 0
    try:
        packages = getattr(load_info, "load_packages", []) or []
        for pkg in packages:
            jobs = getattr(pkg, "jobs", [])
            if isinstance(jobs, dict):
                all_jobs = []
                for job_list in jobs.values():
                    if isinstance(job_list, list):
                        all_jobs.extend(job_list)
                jobs = all_jobs
            for job in jobs:
                rows += getattr(job, "metrics", {}).get("items", 0)
    except Exception as e:
        context.log.error(f"⚠️ Failed to extract metrics from load_info: {e}")
    
    context.log.info(f"✅ Chargement terminé: {rows:,} lignes chargées pour l'année {year}")
    
    return Output(
        {"rows_loaded": rows, "year": year},
        metadata={
            "rows_loaded": MetadataValue.int(rows),
            "partition": MetadataValue.text(year),
            "execution_time_seconds": MetadataValue.float(elapsed_time),
        }
    )


# ============================================================================
# DAILY ASSETS (Incremental - for scheduled streaming)
# ============================================================================

def create_piezometry_chroniques_daily_resource(days_back: int = 7, dagster_context=None):
    """
    Factory for daily piezometry chroniques resource.
    Fetches last N days of data for incremental loading.
    """
    config = _load_config("piezometry_chroniques")
    
    @dlt.resource(
        name="piezometry_chroniques_raw",
        write_disposition="append",
        parallelized=False
    )
    def _resource():
        logger = dagster_context.log if dagster_context else None
        log = logger.info if logger else print
        
        log(f"🔄 [DAILY] Démarrage du chargement incrémental ({days_back} jours)...")
        
        station_codes = _fetch_distinct_column_values(
            "piezometry_stations", 
            "code_bss", 
            logger=logger
        )
        
        if not station_codes:
            log("⚠️ Aucun code de station trouvé!")
            return

        log(f"📊 [DAILY] {len(station_codes):,} stations à traiter")
        
        yield from hubeau_chroniques_daily(
            config, 
            days_back=days_back,
            station_codes=station_codes,
            dagster_context=dagster_context
        )
    
    return _resource


def create_hydrometry_obs_daily_resource(days_back: int = 7, dagster_context=None):
    """
    Factory for daily hydrometry observations resource.
    Fetches last N days of data for incremental loading.
    """
    config = _load_config("hydrometry_obs_elab")
    
    @dlt.resource(
        name="hydrometry_obs_elab_raw",
        write_disposition="append",
        parallelized=False
    )
    def _resource():
        logger = dagster_context.log if dagster_context else None
        log = logger.info if logger else print
        
        log(f"🔄 [DAILY] Démarrage du chargement incrémental ({days_back} jours)...")
        
        station_codes = _fetch_distinct_column_values(
            "hydrometry_stations",
            "code_station",
            logger=logger
        )
        
        if not station_codes:
            log("⚠️ Aucun code de station trouvé!")
            return

        log(f"📊 [DAILY] {len(station_codes):,} stations à traiter")
        
        yield from hubeau_chroniques_daily(
            config, 
            days_back=days_back,
            station_codes=station_codes,
            dagster_context=dagster_context
        )
    
    return _resource


@asset(
    compute_kind="dlt",
    group_name="piezometry_chroniques_daily",
    deps=["piezometry_stations_raw"]
)
def piezometry_chroniques_daily_raw(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Piezometry chroniques - Daily incremental load (last 7 days).
    Used by scheduled daily pipeline.
    """
    import time
    
    context.log.info("📅 [DAILY] Chargement incrémental piézométrie")
    
    pipeline = _create_pipeline("hubeau_piezometry_chroniques_daily")
    resource = create_piezometry_chroniques_daily_resource(days_back=7, dagster_context=context)
    
    start_time = time.time()
    load_info = pipeline.run(resource)
    elapsed_time = time.time() - start_time
    
    context.log.info(f"📋 DLT Load Info:\n{load_info}")
    
    rows = 0
    try:
        packages = getattr(load_info, "load_packages", []) or []
        for pkg in packages:
            jobs = getattr(pkg, "jobs", [])
            if isinstance(jobs, dict):
                all_jobs = []
                for job_list in jobs.values():
                    if isinstance(job_list, list):
                        all_jobs.extend(job_list)
                jobs = all_jobs
            for job in jobs:
                rows += getattr(job, "metrics", {}).get("items", 0)
    except Exception as e:
        context.log.error(f"⚠️ Failed to extract metrics: {e}")
    
    context.log.info(f"✅ [DAILY] Terminé: {rows:,} lignes chargées en {elapsed_time:.1f}s")
    
    return Output(
        {"rows_loaded": rows, "mode": "daily"},
        metadata={
            "rows_loaded": MetadataValue.int(rows),
            "mode": MetadataValue.text("daily_incremental"),
            "execution_time_seconds": MetadataValue.float(elapsed_time),
        }
    )


@asset(
    compute_kind="dlt",
    group_name="hydrometry_chroniques_daily",
    deps=["hydrometry_stations_raw"]
)
def hydrometry_obs_daily_raw(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Hydrometry observations - Daily incremental load (last 7 days).
    Used by scheduled daily pipeline.
    """
    import time
    
    context.log.info("📅 [DAILY] Chargement incrémental hydrométrie")
    
    pipeline = _create_pipeline("hubeau_hydrometry_obs_daily")
    resource = create_hydrometry_obs_daily_resource(days_back=7, dagster_context=context)
    
    start_time = time.time()
    load_info = pipeline.run(resource)
    elapsed_time = time.time() - start_time
    
    context.log.info(f"📋 DLT Load Info:\n{load_info}")
    
    rows = 0
    try:
        packages = getattr(load_info, "load_packages", []) or []
        for pkg in packages:
            jobs = getattr(pkg, "jobs", [])
            if isinstance(jobs, dict):
                all_jobs = []
                for job_list in jobs.values():
                    if isinstance(job_list, list):
                        all_jobs.extend(job_list)
                jobs = all_jobs
            for job in jobs:
                rows += getattr(job, "metrics", {}).get("items", 0)
    except Exception as e:
        context.log.error(f"⚠️ Failed to extract metrics: {e}")
    
    context.log.info(f"✅ [DAILY] Terminé: {rows:,} lignes chargées en {elapsed_time:.1f}s")
    
    return Output(
        {"rows_loaded": rows, "mode": "daily"},
        metadata={
            "rows_loaded": MetadataValue.int(rows),
            "mode": MetadataValue.text("daily_incremental"),
            "execution_time_seconds": MetadataValue.float(elapsed_time),
        }
    )
