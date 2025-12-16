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
        dataset_name=os.environ.get("DLT_BRONZE_DATASET", "staging"),
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
    """DLT Source for Piezometry Stations (FULL load)."""
    return create_referentiel_source(
        name="hubeau_piezometry_stations",
        config=_load_config("piezometry_stations"),
        resource_name="piezometry_stations_raw"
    )()

# ============================================================================
# LAZY LOADING RESOURCE - BEST PRACTICE
# ============================================================================

def _fetch_stations_from_pg(logger=None) -> List[str]:
    """
    Helper to fetch station codes directly from PG at runtime.
    Uses standard env vars available in the container.
    """
    log = logger.info if logger else print
    log("🔍 Récupération des codes de stations depuis PostgreSQL...")
    try:
        host = os.environ.get("PG_HOST", "postgres")
        db = os.environ.get("PG_DB", "postgres")
        user = os.environ.get("PG_USER", "postgres")
        log(f"📡 Connexion à PostgreSQL: {host}/{db} (user: {user})")
        
        conn = psycopg2.connect(
            host=host,
            port=int(os.environ.get("PG_PORT", "5432")),
            database=db,
            user=user,
            password=os.environ.get("PG_PASSWORD"),
        )
        with conn:
            log("✅ Connexion établie. Exécution de la requête...")
            with conn.cursor() as cur:
                # Select distinct codes from bronze/staging table
                schema = os.environ.get("DLT_BRONZE_DATASET", "staging")
                query = f'SELECT DISTINCT "code_bss" FROM "{schema}"."piezometry_stations_raw" WHERE "code_bss" IS NOT NULL'
                cur.execute(query)
                results = [str(row[0]) for row in cur.fetchall()]
                log(f"✅ {len(results):,} codes de stations récupérés depuis PostgreSQL")
                return results
    except Exception as e:
        error_msg = f"❌ Erreur lors de la récupération des stations depuis PG: {e}"
        if logger:
            logger.error(error_msg)
        else:
            print(error_msg, flush=True)
        return []

def create_piezometry_chroniques_resource(year: str, dagster_context=None):
    """
    Factory that creates a resource with NO data arguments.
    The resource fetches its own data (stations) when it starts running.
    """
    config = _load_config("piezometry_chroniques")
    
    @dlt.resource(
        name="piezometry_chroniques_raw",
        write_disposition="append",
        parallelized=False
    )
    def _resource():
        # 1. LAZY LOAD: Fetch stations NOW (Runtime), inside the generator
        # This avoids passing 23k items to the decorator configuration
        # Resolve logger from context if available
        logger = dagster_context.log if dagster_context else None
        log = logger.info if logger else print

        log("🔄 Démarrage du générateur de ressource DLT...")
        
        station_codes = _fetch_stations_from_pg(logger=logger)
        
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
    """DLT Source for Hydrometry Sites (FULL load)."""
    return create_referentiel_source(
        name="hubeau_hydrometry_sites",
        config=_load_config("hydrometry_sites"),
        resource_name="hydrometry_sites_raw"
    )()


@dlt.source(name="hubeau_hydrometry_stations")
def hydrometry_stations_source():
    """DLT Source for Hydrometry Stations (FULL load)."""
    return create_referentiel_source(
        name="hubeau_hydrometry_stations",
        config=_load_config("hydrometry_stations"),
        resource_name="hydrometry_stations_raw"
    )()


@dlt.source(name="hubeau_hydrometry_obs_elab")
def hydrometry_obs_elab_source(year: str):
    """DLT Source for Hydrometry Observations (YEAR partition)."""
    config = _load_config("hydrometry_obs_elab")
    
    @dlt.resource(
        name="hydrometry_obs_elab_raw",
        write_disposition="append",
        parallelized=False
    )
    def _resource():
        yield from hubeau_chroniques_year(config, year=year, station_codes=[]) # Hydrometry uses simpler logic often
    
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
    year = context.partition_key
    pipeline = _create_pipeline(f"hubeau_hydrometry_obs_elab_{year}")
    
    # Hydrometry logic might differ slightly but follows same pattern
    # For now assuming simple fetch
    config = _load_config("hydrometry_obs_elab")
    
    @dlt.resource(name="hydrometry_obs_elab_raw", write_disposition="append")
    def _res():
        yield from hubeau_chroniques_year(config, year, station_codes=[]) 
    
    load_info = pipeline.run(_res)
    
    # Metrics extraction
    rows = 0
    for pkg in getattr(load_info, "load_packages", []) or []:
        for job in getattr(pkg, "jobs", []) or []:
            rows += getattr(job, "metrics", {}).get("items", 0)
            
    return Output({}, metadata={"year": year, "rows_loaded": MetadataValue.int(rows)})
