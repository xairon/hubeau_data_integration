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

    # Temperature
    "temperature_chroniques": ["temperature_stations_csv"],

    # Hydrobio
    "hydrobio_indices": ["hydrobio_stations_csv"],
    "hydrobio_taxons": ["hydrobio_stations_csv"],

    # Ecoulement (dépend de stations ET campagnes)
    "ecoulement_observations": ["ecoulement_stations_csv", "ecoulement_campagnes_csv"],

    # Prelevements (dépend de ouvrages ET points)
    "prelevements_chroniques": ["prelevements_ouvrages_csv", "prelevements_points_csv"],
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


def log_memory_usage(context: AssetExecutionContext, label: str):
    """
    Log la consommation mémoire actuelle du process avec détails

    Nécessite: pip install psutil (déjà dans requirements.txt normalement)

    Args:
        context: Contexte Dagster pour logging
        label: Label descriptif pour le log
    """
    try:
        import psutil
        import os

        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()

        # RSS = Resident Set Size (RAM physique utilisée)
        mem_mb = mem_info.rss / 1024 / 1024

        # VMS = Virtual Memory Size (RAM + Swap)
        vms_mb = mem_info.vms / 1024 / 1024

        # Percent de RAM système utilisé
        system_mem = psutil.virtual_memory()
        system_percent = system_mem.percent
        system_available_mb = system_mem.available / 1024 / 1024

        context.log.info(
            f"[MEM] {label}: "
            f"Process={mem_mb:.1f} MB RSS, "
            f"{vms_mb:.1f} MB VMS | "
            f"System={system_percent:.1f}% used, "
            f"{system_available_mb:.1f} MB available"
        )

        # Warning si process > 3 GB (proche de la limite problématique)
        if mem_mb > 3000:
            context.log.warning(
                f"⚠️ [MEM] Process utilise {mem_mb:.1f} MB (> 3 GB) - "
                f"Risque OOM si d'autres assets en parallèle !"
            )

    except ImportError:
        # psutil pas installé, log warning une fois
        context.log.warning(f"[MEM] psutil non installé, monitoring RAM désactivé")
    except Exception as e:
        # Autre erreur, log debug mais ne crash pas
        context.log.debug(f"[MEM] Erreur monitoring mémoire: {e}")


def batch_iterator_with_flush(source, batch_size: int, micro_batch_size: int):
    """
    Generator qui yield des micro-batches au lieu d'accumuler en RAM

    Args:
        source: Iterator de records (dict)
        batch_size: Taille batch cible pour métrique (info only)
        micro_batch_size: Taille réelle des micro-batches yield

    Yields:
        List[Dict] de taille micro_batch_size (sauf dernier)
    """
    micro_batch = []
    
    # ✅ Extraire les données depuis DLT source correctement
    resource_name_key = list(source.resources.keys())[0]  # Premier (et seul) resource
    data_iterator = source.resources[resource_name_key]()  # Appeler pour obtenir l'iterator

    for record in data_iterator:
        # Validation type
        if not isinstance(record, dict):
            continue
            
        micro_batch.append(record)

        # Yield quand micro-batch plein
        if len(micro_batch) >= micro_batch_size:
            yield micro_batch
            micro_batch = []  # Reset immédiat

    # Yield reste (< micro_batch_size)
    if micro_batch:
        yield micro_batch


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

        # ✨ Détecter le mode depuis la partition si disponible
        # NOTE: Config est frozen (Pydantic), on doit créer un NOUVEL objet au lieu de muter
        if context.has_partition_key:
            partition = context.partition_key
            context.log.info(f"📋 Partition sélectionnée: {partition}")

            if partition == "full":
                config = IngestionConfig(
                    mode="full",
                    year=None,
                    incremental_days=config.incremental_days
                )
            elif partition == "incremental":
                config = IngestionConfig(
                    mode="incremental",
                    year=None,
                    incremental_days=config.incremental_days
                )
            else:
                # C'est une année (2024, 2023, etc.)
                try:
                    year_value = int(partition)
                    config = IngestionConfig(
                        mode="year",
                        year=year_value,
                        incremental_days=config.incremental_days
                    )
                except ValueError:
                    context.log.error(f"❌ Partition invalide: {partition} (attendu: full, incremental, ou YYYY)")
                    raise ValueError(f"Partition invalide: {partition}")

        # Validation
        if config.mode == "year" and not config.year:
            raise ValueError("Mode YEAR necessite le parametre 'year'")

        # Si resource ne supporte pas les filtres date, forcer mode FULL
        if not supports_date_filter and config.mode in ["year", "incremental"]:
            context.log.warning(
                f"{resource_name} ne supporte pas les filtres date. "
                f"Passage en mode FULL."
            )
            config = IngestionConfig(
                mode="full",
                year=None,
                incremental_days=config.incremental_days
            )

        # Charger config YAML
        yaml_config = load_yaml_config(resource_name)

        # Logs
        context.log.info(f"🚀 Ingestion: {resource_name}")
        context.log.info(f"   Mode: {config.mode}")
        if config.mode == "year":
            context.log.info(f"   Annee: {config.year}")
        elif config.mode == "incremental":
            context.log.info(f"   Derniers {config.incremental_days} jours")

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

        # Source avec mode
        mode_enum = IngestionMode[config.mode.upper()]

        source = hubeau_csv_source(
            resource_name=yaml_config['resource']['name'],
            endpoint=yaml_config['resource']['endpoint'],
            base_url=yaml_config['resource']['base_url'],
            primary_key=yaml_config['resource']['primary_key'],
            performance_config=yaml_config['performance'],
            default_params=yaml_config['extraction'].get('default_params', {}),
            mode=mode_enum,
            year=config.year,
            incremental_days=config.incremental_days,
            use_station_slicing=use_station_slicing
        )

        # ✅ OPTIMISATION MÉMOIRE V2: Micro-batching + Chunking + Generator pattern
        # Évite accumulation RAM en flushant toutes les 1000 records
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

            # ✅ BATCH_SIZE depuis YAML config - désormais AUGMENTÉ car mémoire sécurisée par micro-batching
            # Voir configs/hubeau/*.yml → performance.batch_size (augmenté de 3-10x vs anciennes valeurs)
            BATCH_SIZE = yaml_config['performance'].get('batch_size', 50000)

            # ✅ MICRO_BATCH_SIZE: Flush PostgreSQL toutes les N records pour économiser RAM
            # RAM peak = micro_batch_size × ~1KB/record = 1000 × 1KB = ~1 MB (au lieu de 50 MB avant)
            MICRO_BATCH_SIZE = 1000

            # Log niveau selon batch size
            if BATCH_SIZE <= 5000:
                context.log.info(f"Dataset: {resource_name} → BATCH_SIZE={BATCH_SIZE:,} (micro-batch={MICRO_BATCH_SIZE})")
            elif BATCH_SIZE <= 20000:
                context.log.info(f"Dataset moyen: {resource_name} → BATCH_SIZE={BATCH_SIZE:,} (micro-batch={MICRO_BATCH_SIZE})")
            else:
                context.log.info(f"Dataset large (optimisé): {resource_name} → BATCH_SIZE={BATCH_SIZE:,} (micro-batch={MICRO_BATCH_SIZE})")

            context.log.info(f"📥 Streaming par micro-batch (BATCH={BATCH_SIZE:,}, MICRO={MICRO_BATCH_SIZE}, disposition={write_disposition})...")

            # Log mémoire initiale
            log_memory_usage(context, "Avant ingestion")

            total_records = 0
            micro_batch_count = 0
            is_first_write = True  # Track first write for TRUNCATE (si replace)

            # ✅ GENERATOR PATTERN: Pas d'accumulation, yield au fur et à mesure
            for micro_batch in batch_iterator_with_flush(source, BATCH_SIZE, MICRO_BATCH_SIZE):
                micro_batch_count += 1

                # ✅ LOGIQUE CORRIGÉE pour micro-batching:
                # - Mode "replace": premier micro-batch fait TRUNCATE+INSERT, suivants font APPEND
                # - Mode "merge": TOUS les micro-batches font MERGE (pour éviter doublons)
                if write_disposition == "replace":
                    batch_write_disposition = "replace" if is_first_write else "append"
                    is_first_write = False
                else:
                    # Mode merge/year/incremental: TOUJOURS merge pour éviter doublons
                    batch_write_disposition = write_disposition

                # Log tous les 10 micro-batches (évite spam)
                if micro_batch_count % 10 == 0:
                    context.log.info(f"💾 Micro-batch {micro_batch_count}: {len(micro_batch):,} records (cumul: {total_records:,})")

                load_start = time.time()
                postgres_bulk_destination.load_batch(
                    table_name=table_name,
                    data=micro_batch,
                    write_disposition=batch_write_disposition,
                    primary_keys=primary_keys if primary_keys else None,
                    column_mappings=None
                )
                load_duration = time.time() - load_start

                total_records += len(micro_batch)

                # Log verbose pour premiers/derniers micro-batches
                if micro_batch_count <= 3 or micro_batch_count % 50 == 0:
                    throughput = len(micro_batch) / load_duration if load_duration > 0 else 0
                    context.log.info(f"✅ Micro-batch {micro_batch_count}: {len(micro_batch):,} records en {load_duration:.2f}s ({throughput:.0f} rec/s)")

                # GARBAGE COLLECTION AGRESSIF après chaque micro-batch
                import gc
                gc.collect()

                # Log mémoire tous les 50 micro-batches
                if micro_batch_count % 50 == 0:
                    log_memory_usage(context, f"Après {micro_batch_count} micro-batches")

            elapsed = time.time() - start_time

            # Log mémoire finale
            log_memory_usage(context, "Après ingestion complète")

            # Log résultats finaux
            if total_records > 0:
                global_throughput = total_records / elapsed if elapsed > 0 else 0
                context.log.info(f"✅ Extraction et chargement terminés en {elapsed:.2f}s")
                context.log.info(f"📊 Total: {total_records:,} records en {micro_batch_count} micro-batch(s)")
                context.log.info(f"⚡ Performance globale: {global_throughput:.0f} records/seconde")
            else:
                context.log.warning(f"⚠️  Table {table_name} vide après ingestion ! Vérifier: API Hub'Eau, filtres date, ou réseau.")

            return Output(
                value={"records": total_records, "batches": micro_batch_count},
                metadata={
                    "mode": MetadataValue.text(config.mode),
                    "year": MetadataValue.int(config.year) if config.year else None,
                    "incremental_days": MetadataValue.int(config.incremental_days) if config.mode == "incremental" else None,
                    "rows_loaded": MetadataValue.int(total_records),
                    "batch_count": MetadataValue.int(micro_batch_count),
                    "duration_seconds": MetadataValue.float(round(elapsed, 2)),
                    "throughput_rows_per_sec": MetadataValue.float(round(total_records/elapsed, 2)) if elapsed > 0 else None
                }
            )

        except Exception as e:
            context.log.error(f"❌ Erreur: {e}")
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
