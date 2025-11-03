"""
Assets Hub'Eau - Ingestion PostgreSQL

Pipeline simple d'ingestion des données Hub'Eau vers PostgreSQL avec support multi-mode :
- FULL : Tout l'historique
- YEAR : Une année spécifique
- INCREMENTAL : Derniers N jours
"""

import os
import dlt
import yaml
import time
import tempfile
from pathlib import Path
from datetime import datetime
from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
    Config,
    DynamicPartitionsDefinition,
    StaticPartitionsDefinition,
    AssetKey,
)
from pydantic import Field

# Dagster 1.11.0+ renamed FreshnessPolicy to LegacyFreshnessPolicy
try:
    from dagster import FreshnessPolicy
except ImportError:
    from dagster import LegacyFreshnessPolicy as FreshnessPolicy
from typing import Optional, Literal, Dict, Any

from hubeau_pipeline.sources.hubeau_csv_source import hubeau_csv_source, IngestionMode
from hubeau_pipeline.destinations.postgres_optimized_v2 import get_postgres_destination
from hubeau_pipeline.resources import PostgreSQLResource
from hubeau_pipeline.utils.schema_loader import ensure_table_exists
from hubeau_pipeline.schema.hubeau_type_mappings import TABLE_FK_MAPPING


# ============================================================================
# PARTITIONS DÉFINITIONS
# ============================================================================

# Partitions pour les modes (avec années prédéfinies + possibilité d'ajout dynamique)
# Les années peuvent être ajoutées dynamiquement via API ou UI
HUBEAU_PARTITIONS = DynamicPartitionsDefinition(name="hubeau_time_partitions")

# Partitions statiques pour facilité d'usage
MODE_PARTITIONS = StaticPartitionsDefinition([
    "full",           # Tout l'historique
    "incremental",    # Derniers 2 jours
    "2024",          # Année 2024
    "2023",          # Année 2023
    "2022",          # Année 2022
    "2021",          # Année 2021
    "2020",          # Année 2020
])


# ============================================================================
# MAPPING PARAMÈTRES API PAR RESOURCE
# ============================================================================
# Chaque endpoint Hub'Eau a ses propres noms de paramètres pour filtrer par date
# Format: resource_name → (nom_param_min, nom_param_max, utilise_annee_seule)
DATE_PARAM_MAPPING = {
    "ecoulement_observations": ("date_observation_min", "date_observation_max", False),
    "piezometry_chroniques": ("date_debut_mesure", "date_fin_mesure", False),
    "hydrometry_obs_elab": ("date_debut_obs_elab", "date_fin_obs_elab", False),
    "quality_groundwater_analyses": ("date_debut_prelevement", "date_fin_prelevement", False),
    "quality_rivers_analyses": ("date_debut_prelevement", "date_fin_prelevement", False),  # ✅ CORRECTION: date_debut/fin au lieu de _min/_max
    "quality_rivers_conditions": ("date_debut_prelevement", "date_fin_prelevement", False),  # ✅ CORRECTION: date_debut/fin au lieu de _min/_max
    "quality_rivers_operations": ("date_debut_prelevement", "date_fin_prelevement", False),  # ✅ CORRECTION: date_debut/fin au lieu de _min/_max
    "temperature_chroniques": ("date_debut_mesure", "date_fin_mesure", False),  # ✅ CORRECTION: date_debut_mesure au lieu de date_mesure_temp_min
    "hydrobio_indices": ("date_debut_prelevement", "date_fin_prelevement", False),  # ✅ CORRECTION: date_debut/fin au lieu de _min/_max
    "hydrobio_taxons": ("date_debut_prelevement", "date_fin_prelevement", False),  # ✅ CORRECTION: date_debut/fin au lieu de _min/_max
    "prelevements_chroniques": ("annee", None, True),  # Cas spécial: utilise année uniquement
}


# ============================================================================
# ASSET DEPENDENCIES
# ============================================================================
# Mapping resource_name → assets stations dont il dépend
ASSET_DEPENDENCIES = {
    # Piezometry
    "piezometry_chroniques": ["piezometry_stations_csv"],

    # Quality Rivers
    "quality_rivers_analyses": ["quality_rivers_stations_csv"],
    "quality_rivers_conditions": ["quality_rivers_stations_csv"],
    "quality_rivers_operations": ["quality_rivers_stations_csv"],

    # Quality Groundwater
    "quality_groundwater_analyses": ["quality_groundwater_stations_csv"],

    # Hydrometry (dépend de sites ET stations)
    "hydrometry_obs_elab": ["hydrometry_sites_csv", "hydrometry_stations_csv"],
    # Assurer que les stations ne chargent qu'après les sites (FK code_site)
    "hydrometry_stations": ["hydrometry_sites_csv"],

    # Temperature
    "temperature_chroniques": ["temperature_stations_csv"],

    # Hydrobio
    "hydrobio_indices": ["hydrobio_stations_csv"],
    "hydrobio_taxons": ["hydrobio_stations_csv"],

    # Ecoulement (dépend de stations ET campagnes)
    "ecoulement_observations": ["ecoulement_stations_csv", "ecoulement_campagnes_csv"],

    # Prelevements (dépend de ouvrages ET points)
    "prelevements_chroniques": ["prelevements_ouvrages_csv", "prelevements_points_csv"],
    # Assurer que les points sont chargés après les ouvrages (FK code_ouvrage)
    "prelevements_points": ["prelevements_ouvrages_csv"],
}


# ============================================================================
# FRESHNESS POLICIES
# ============================================================================
# Alerte si chroniques/analyses pas mises à jour depuis > 48h
CHRONIQUES_FRESHNESS_POLICY = FreshnessPolicy(
    maximum_lag_minutes=60 * 48,  # 48 heures
    cron_schedule="0 2 * * *",    # Vérifié quotidiennement à 02h00
)


class IngestionConfig(Config):
    """Configuration pour les assets avec mode selectionnable"""
    mode: Literal["full", "year", "incremental"] = Field(
        default="full",
        description="Mode d'ingestion"
    )
    year: Optional[int] = Field(
        default=None,
        description="Annee a ingerer (mode YEAR uniquement)"
    )
    incremental_days: int = Field(
        default=2,
        description="Nombre de jours a recuperer (mode INCREMENTAL)"
    )


def load_yaml_config(resource_name: str) -> Dict:
    """Charge la configuration YAML d'une ressource"""
    config_path = Path(f"configs/hubeau/{resource_name}.yml")
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def count_rows_in_postgres(table_name: str, pg: PostgreSQLResource, schema: str = "hubeau") -> int:
    """
    Compte les lignes dans une table PostgreSQL.

    Utilisé pour obtenir le vrai nombre de lignes chargées,
    car DLT load_info ne retourne pas toujours les métriques correctement.

    Args:
        table_name: Nom de la table
        pg: PostgreSQL resource (injected by Dagster)
        schema: Schéma PostgreSQL (défaut: hubeau)

    Returns:
        Nombre de lignes dans la table, ou 0 si erreur/table inexistante
    """
    try:
        with pg.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
                count = cur.fetchone()[0]
            return count

    except Exception as e:
        # Si erreur (table n'existe pas encore, connexion échouée, etc.)
        return 0


# Fonctions de monitoring simplifiées - plus utilisées avec 32GB RAM
# (gardées pour compatibilité mais peuvent être supprimées)


def create_csv_asset(resource_name: str, supports_date_filter: bool = True, use_station_slicing: bool = False):
    """
    Factory pour creer un asset DLT CSV multi-mode

    Args:
        resource_name: Nom de la ressource (sans suffixe _csv)
        supports_date_filter: True si l'endpoint supporte les filtres date
        use_station_slicing: True pour slicing par station (piezometry_chroniques)
    """

    group_name = resource_name.split('_')[0]
    asset_name = f"{resource_name}_csv"  # Add _csv suffix to asset name

    # ✅ Détecter les dépendances pour cet asset
    asset_deps = []
    if resource_name in ASSET_DEPENDENCIES:
        asset_deps = [AssetKey(dep_name) for dep_name in ASSET_DEPENDENCIES[resource_name]]

    @asset(
        name=asset_name,
        group_name=group_name,
        compute_kind="dlt",
        op_tags={"format": "csv", "source": "hubeau"},
        partitions_def=MODE_PARTITIONS if supports_date_filter else None,  # Partitions seulement si filtre date supporté
        deps=asset_deps,  # ← Dépendances vers assets stations
        # NOTE: freshness_policy retiré - incompatible avec Dagster 1.11.0+
        # TODO: Migrer vers AutomationCondition + freshness_checks
        metadata={
            "partition_type": "time_based" if supports_date_filter else "none",
            "supports_incremental": supports_date_filter,
            "description": f"Ingestion Hub'Eau pour {resource_name}",
            "depends_on": ASSET_DEPENDENCIES.get(resource_name, []),  # ← Metadata dependencies
            "freshness_check": "48h" if supports_date_filter else "none"  # ← Metadata freshness (info only)
        }
    )
    def csv_asset(
        context: AssetExecutionContext,
        config: IngestionConfig
    ) -> Output:
        """
        Asset DLT CSV avec modes :
        - FULL : Tout l'historique
        - YEAR : Une annee specifique
        - INCREMENTAL : Derniers N jours
        """

        # Déterminer mode d'ingestion depuis partition
        if context.has_partition_key:
            context.log.info(f"INFO: Partition detected: {context.partition_key}")

        # Étape 1: S'assurer que la table existe (créer via SQL si nécessaire)
        table_name = resource_name
        conn_params = {
            "host": os.getenv("PG_HOST", "postgres"),
            "port": int(os.getenv("PG_PORT", "5432")),
            "database": os.getenv("PG_DB", "postgres"),
            "user": os.getenv("PG_USER", "postgres"),
            "password": os.getenv("PG_PASSWORD", ""),
        }

        try:
            ensure_table_exists(table_name, conn_params)
            context.log.info(f"INFO: Table hubeau.{table_name} ready")
        except RuntimeError as e:
            context.log.error(f"ERROR: Failed to create table: {e}")
            raise

        # Détecter mode depuis partition Dagster
        if context.has_partition_key:
            partition = context.partition_key
            if partition == "full":
                config = IngestionConfig(mode="full", year=None, incremental_days=config.incremental_days)
            elif partition == "incremental":
                config = IngestionConfig(mode="incremental", year=None, incremental_days=config.incremental_days)
            else:
                # Année spécifique (2024, 2023, etc.)
                try:
                    year_value = int(partition)
                    config = IngestionConfig(mode="year", year=year_value, incremental_days=config.incremental_days)
                except ValueError:
                    raise ValueError(f"Invalid partition: {partition} (expected: full, incremental, or YYYY)")

        # Validation
        if config.mode == "year" and not config.year:
            raise ValueError("Year mode requires 'year' parameter")

        # Forcer mode FULL pour ressources sans support filtre date
        if not supports_date_filter and config.mode in ["year", "incremental"]:
            context.log.warning(f"WARNING: {resource_name} does not support date filters, switching to FULL mode")
            config = IngestionConfig(mode="full", year=None, incremental_days=config.incremental_days)

        # Charger config YAML
        yaml_config = load_yaml_config(resource_name)

        # Log mode d'ingestion
        context.log.info(f"INFO: Starting ingestion - resource={resource_name}, mode={config.mode}, year={config.year if config.mode=='year' else 'N/A'}")

        # Use EXACT same PostgreSQL destination as old JSON system
        postgres_config = {
            "dataset_name": os.getenv("HUBEAU_SCHEMA", "hubeau")
        }
        destination = get_postgres_destination(postgres_config)

        # Use temp directory for pipelines like old system
        pipelines_dir = os.path.join(tempfile.gettempdir(), "dlt_pipelines")
        os.makedirs(pipelines_dir, exist_ok=True)

        # Pipeline DLT using same method as JSON assets
        pipeline = dlt.pipeline(
            pipeline_name=f"hubeau_{resource_name}_csv_{config.mode}",
            destination=destination,
            dataset_name=postgres_config["dataset_name"],
            pipelines_dir=pipelines_dir,
            full_refresh=False
        )

        # Préparer filtre FK si applicable (éviter orphelins)
        parent_keys: Optional[set] = None
        parent_table: Optional[str] = None  # Définir au niveau global pour scope dans boucle
        child_col: Optional[str] = None      # Définir au niveau global pour scope dans boucle
        fk_list = TABLE_FK_MAPPING.get(resource_name)

        # Charger clés FK si nécessaire
        if fk_list is not None and len(fk_list) > 0:
            import psycopg2
            child_col, parent_table, parent_col = fk_list[0]
            context.log.info(f"INFO: FK filter enabled: {table_name}.{child_col} -> {parent_table}.{parent_col}")
            try:
                _conn = psycopg2.connect(**conn_params)
                _cur = _conn.cursor()
                _cur.execute(f"SELECT {parent_col} FROM hubeau.{parent_table}")
                parent_keys = {row[0] for row in _cur.fetchall()}
                _cur.close()
                _conn.close()
                context.log.info(f"INFO: Loaded {len(parent_keys)} parent keys from {parent_table}.{parent_col}")
            except Exception as fk_e:
                raise RuntimeError(f"Failed to load parent keys for FK filtering: {parent_table}.{parent_col}: {fk_e}")

        # ✅ BYPASS DLT - Use direct pagination to get actual lists
        # Import pagination functions directly (no wrapper)
        from hubeau_pipeline.sources.hubeau_csv_source import _paginate_csv, _paginate_with_station_slicing, HubeauAPIClient

        # Build config dict for the raw iterator
        config_dict = {
            'resource': yaml_config['resource'],
            'extraction': yaml_config['extraction'],
            'performance': yaml_config['performance'],
            'pagination': yaml_config.get('pagination', {})
        }

        # Ajouter paramètres date selon mode
        if config.mode == "year" and config.year and resource_name in DATE_PARAM_MAPPING:
            param_min, param_max, use_year_only = DATE_PARAM_MAPPING[resource_name]
            if use_year_only:
                config_dict['extraction']['default_params'] = {
                    **config_dict['extraction'].get('default_params', {}),
                    param_min: config.year
                }
            else:
                config_dict['extraction']['default_params'] = {
                    **config_dict['extraction'].get('default_params', {}),
                    param_min: f"{config.year}-01-01",
                    param_max: f"{config.year}-12-31"
                }
            context.log.info(f"INFO: Date filters applied for year {config.year}")

        elif config.mode == "incremental" and resource_name in DATE_PARAM_MAPPING:
            from datetime import datetime, timedelta
            today = datetime.now()
            start_date = today - timedelta(days=config.incremental_days)
            param_min, param_max, use_year_only = DATE_PARAM_MAPPING[resource_name]

            if use_year_only:
                config_dict['extraction']['default_params'] = {
                    **config_dict['extraction'].get('default_params', {}),
                    param_min: today.year
                }
            else:
                config_dict['extraction']['default_params'] = {
                    **config_dict['extraction'].get('default_params', {}),
                    param_min: start_date.strftime("%Y-%m-%d"),
                    param_max: today.strftime("%Y-%m-%d")
                }
            context.log.info(f"INFO: Incremental mode - last {config.incremental_days} days")

        # Add station slicing configuration if needed
        if use_station_slicing:
            config_dict['pagination']['station_field'] = 'code_bss'
            if 'piezometry' in resource_name:
                config_dict['pagination']['stations_endpoint'] = '/stations.csv'

        # ✅ SIMPLIFIÉ: Pas de micro-batching - pages complètes directement en base
        # L'API retourne des pages de ~5k records, on les charge directement
        start_time = time.time()

        try:
            # Import de notre custom destination optimisée avec COPY PostgreSQL natif
            from hubeau_pipeline.destinations.postgres_optimized_v2 import postgres_bulk_destination_v2 as postgres_bulk_destination

            table_name = yaml_config['resource']['name']
            primary_keys = yaml_config['resource'].get('primary_key', [])
            if isinstance(primary_keys, str):
                primary_keys = [primary_keys]

            # ✅ Déterminer write_disposition:
            # - FULL: replace (TRUNCATE + INSERT) - Efface tout et recommence
            # - YEAR/INCREMENTAL: merge (UPSERT via PK) - Met à jour sans doublons
            write_disposition = "replace" if config.mode == "full" else "merge"

            context.log.info(f"INFO: Loading data (disposition={write_disposition})")

            total_records = 0
            page_count = 0
            is_first_write = True

            # ✅ CRITICAL: Thread-safe first_batch flag for parallel fetching
            # Without this, ALL parallel workers think they're first and do DELETE
            from threading import Lock
            first_batch_lock = Lock()
            first_batch_done = [False]  # Mutable container to share state across threads

            # Choisir stratégie de fetching (parallèle vs séquentielle)
            if config.mode in ["year", "full"] and resource_name not in ["piezometry_chroniques"]:
                from hubeau_pipeline.sources.hubeau_csv_parallel import get_parallel_data_iterator
                max_workers = yaml_config.get('performance', {}).get('parallelism', 5)
                context.log.info(f"INFO: Parallel fetching ({max_workers} workers)")
                data_iterator = get_parallel_data_iterator(config_dict, max_workers=max_workers)
            else:
                # Sequential fetching using direct pagination functions
                context.log.info(f"INFO: Sequential fetching")

                # Create HTTP client
                client = HubeauAPIClient(
                    base_url=yaml_config['resource']['base_url'],
                    rate_limit=yaml_config['performance'].get('rate_limit', 2.0)
                )

                endpoint = yaml_config['resource']['endpoint']
                params = config_dict['extraction'].get('default_params', {})

                # Choose pagination strategy
                if 'station_field' in config_dict.get('pagination', {}):
                    # Station slicing (piezometry)
                    data_iterator = _paginate_with_station_slicing(
                        client=client,
                        endpoint=endpoint,
                        params=params,
                        resource_name=resource_name,
                        station_field=config_dict['pagination']['station_field']
                    )
                else:
                    # Standard pagination
                    data_iterator = _paginate_csv(
                        client=client,
                        endpoint=endpoint,
                        params=params,
                        resource_name=resource_name
                    )

            # ✅ SIMPLE: Itérer sur les pages de l'API (~5k records)
            for page_records in data_iterator:
                page_count += 1

                # Now page_records should be a list of dicts as expected!
                # No need to validate type anymore since we control the iterator
                if len(page_records) == 0:
                    continue

                # ✅ Write disposition pour cette page:
                # - Mode "replace": première page fait TRUNCATE+INSERT, suivantes font MERGE (évite doublons API)
                # - Mode "merge": TOUTES les pages font MERGE (évite doublons)
                if write_disposition == "replace":
                    batch_write_disposition = "replace" if is_first_write else "merge"
                    is_first_write = False
                else:
                    batch_write_disposition = write_disposition

                # Normaliser clés (case-insensitive)
                page_records = [
                    {key.lower(): value for key, value in record.items()}
                    for record in page_records
                ]

                # Filtrer NULL dans PRIMARY KEY
                if primary_keys:
                    primary_keys_lower = [pk.lower() for pk in primary_keys]
                    before_pk_filter = len(page_records)
                    page_records = [
                        r for r in page_records
                        if all(
                            r.get(pk) is not None
                            and r.get(pk) != ''
                            and str(r.get(pk)).strip().lower() not in ('null', 'none', 'nan', 'n/a')
                            for pk in primary_keys_lower
                        )
                    ]
                    dropped_pk = before_pk_filter - len(page_records)
                    if dropped_pk > 0:
                        context.log.warning(f"WARNING: Dropped {dropped_pk} records with NULL in PRIMARY KEY")

                # Filtrer lignes orphelines FK
                if parent_keys is not None and fk_list is not None and len(fk_list) > 0:
                    before = len(page_records)
                    import pandas as pd
                    df = pd.DataFrame(page_records)
                    mask = df[child_col].isna() | df[child_col].isin(parent_keys)
                    df_filtered = df[mask]
                    page_records = df_filtered.to_dict('records')
                    dropped = before - len(page_records)
                    if dropped > 0:
                        context.log.warning(f"WARNING: Dropped {dropped} orphan records (FK {child_col} missing in {parent_table})")

                # Charger page en base
                load_start = time.time()
                partition_year = config.year if config.mode == "year" else None

                # ✅ CRITICAL: Thread-safe check for first batch
                # ONLY one thread should do DELETE+COPY, rest should UPSERT
                is_first_batch = False
                if partition_year and batch_write_disposition == "merge":
                    with first_batch_lock:
                        if not first_batch_done[0]:
                            is_first_batch = True
                            first_batch_done[0] = True

                postgres_bulk_destination.load_batch(
                    table_name=table_name,
                    data=page_records,
                    write_disposition=batch_write_disposition,
                    primary_keys=primary_keys if primary_keys else None,
                    column_mappings=None,
                    partition_year=partition_year,
                    is_first_batch=is_first_batch  # ✅ Pass first batch flag for DELETE+COPY optimization
                )
                load_duration = time.time() - load_start
                total_records += len(page_records)

                # Log progression toutes les 10 pages
                if page_count % 10 == 0:
                    context.log.info(f"INFO: Progress - {page_count} pages, {total_records:,} records loaded")

            elapsed = time.time() - start_time

            # Log résultats finaux
            if total_records > 0:
                context.log.info(f"INFO: Completed - {total_records:,} records in {page_count} pages ({elapsed:.2f}s)")
            else:
                context.log.warning(f"WARNING: No records loaded for {table_name}")

            return Output(
                value={"records": total_records, "pages": page_count},
                metadata={
                    "mode": MetadataValue.text(config.mode),
                    "year": MetadataValue.int(config.year) if config.year else None,
                    "incremental_days": MetadataValue.int(config.incremental_days) if config.mode == "incremental" else None,
                    "rows_loaded": MetadataValue.int(total_records),
                    "page_count": MetadataValue.int(page_count),
                    "duration_seconds": MetadataValue.float(round(elapsed, 2)),
                    "throughput_rows_per_sec": MetadataValue.float(round(total_records/elapsed, 2)) if elapsed > 0 else None
                }
            )

        except Exception as e:
            context.log.error(f"ERROR: {e}")
            raise

    return csv_asset


# ========================================
# GENERATION DES ASSETS
# ========================================

# Tables avec support filtre date (chroniques, analyses, observations)
DATE_FILTERABLE_RESOURCES = [
    ("piezometry_chroniques", True),  # (nom, use_station_slicing)
    ("quality_groundwater_analyses", False),
    ("quality_rivers_analyses", False),
    ("quality_rivers_conditions", False),
    ("quality_rivers_operations", False),
    ("temperature_chroniques", False),
    ("hydrometry_obs_elab", False),
    ("hydrobio_indices", False),
    ("hydrobio_taxons", False),
    ("ecoulement_observations", False),
    ("prelevements_chroniques", False)
]

# Tables referentiels (stations, sans filtre date)
REFERENCE_RESOURCES = [
    "piezometry_stations",
    "quality_groundwater_stations",
    "quality_rivers_stations",
    "temperature_stations",
    "hydrometry_sites",
    "hydrometry_stations",
    "hydrobio_stations",
    "ecoulement_stations",
    "ecoulement_campagnes",
    "prelevements_points",
    "prelevements_ouvrages"
]

# Creer assets avec support date
for resource, use_slicing in DATE_FILTERABLE_RESOURCES:
    globals()[f"{resource}_csv"] = create_csv_asset(
        resource,
        supports_date_filter=True,
        use_station_slicing=use_slicing
    )

# Creer assets sans support date (referentiels)
for resource in REFERENCE_RESOURCES:
    globals()[f"{resource}_csv"] = create_csv_asset(
        resource,
        supports_date_filter=False
    )
