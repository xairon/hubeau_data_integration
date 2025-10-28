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

        # ✅ BYPASS DLT - Use direct pagination to get actual lists
        # Import our new direct iterator function
        from hubeau_pipeline.sources.hubeau_csv_source import get_raw_data_iterator

        # Build config dict for the raw iterator
        config_dict = {
            'resource': yaml_config['resource'],
            'extraction': yaml_config['extraction'],
            'performance': yaml_config['performance'],
            'pagination': yaml_config.get('pagination', {})
        }

        # Add mode-specific parameters to extraction config
        if config.mode == "year" and config.year:
            # For YEAR mode, add date filters
            date_field = 'mesure' if 'chronique' in resource_name or 'observation' in resource_name else 'prelevement'
            config_dict['extraction']['default_params'] = {
                **config_dict['extraction'].get('default_params', {}),
                f'date_debut_{date_field}': f"{config.year}-01-01",
                f'date_fin_{date_field}': f"{config.year}-12-31"
            }
        elif config.mode == "incremental":
            # For INCREMENTAL mode, add date filters for last N days
            from datetime import datetime, timedelta
            today = datetime.now()
            start_date = today - timedelta(days=config.incremental_days)
            date_field = 'mesure' if 'chronique' in resource_name or 'observation' in resource_name else 'prelevement'
            config_dict['extraction']['default_params'] = {
                **config_dict['extraction'].get('default_params', {}),
                f'date_debut_{date_field}': start_date.strftime("%Y-%m-%d"),
                f'date_fin_{date_field}': today.strftime("%Y-%m-%d")
            }

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

            context.log.info(f"📥 Streaming pages complètes (~5k records/page, disposition={write_disposition})...")

            total_records = 0
            page_count = 0
            is_first_write = True  # Track first write for TRUNCATE (si replace)

            # ✅ Get raw data iterator - bypasses DLT wrapper
            data_iterator = get_raw_data_iterator(config_dict)

            # ✅ SIMPLE: Itérer sur les pages de l'API (~5k records)
            for page_records in data_iterator:
                page_count += 1

                # Now page_records should be a list of dicts as expected!
                # No need to validate type anymore since we control the iterator
                if len(page_records) == 0:
                    continue

                # ✅ Write disposition pour cette page:
                # - Mode "replace": première page fait TRUNCATE+INSERT, suivantes font APPEND
                # - Mode "merge": TOUTES les pages font MERGE (évite doublons)
                if write_disposition == "replace":
                    batch_write_disposition = "replace" if is_first_write else "append"
                    is_first_write = False
                else:
                    batch_write_disposition = write_disposition

                # Charger page complète en base
                load_start = time.time()
                postgres_bulk_destination.load_batch(
                    table_name=table_name,
                    data=page_records,
                    write_disposition=batch_write_disposition,
                    primary_keys=primary_keys if primary_keys else None,
                    column_mappings=None
                )
                load_duration = time.time() - load_start

                total_records += len(page_records)
                throughput = len(page_records) / load_duration if load_duration > 0 else 0

                # Log progression
                context.log.info(
                    f"📥 Page {page_count}: {len(page_records):,} records en {load_duration:.2f}s "
                    f"({throughput:.0f} rec/s, total: {total_records:,})"
                )

            elapsed = time.time() - start_time

            # Log résultats finaux
            if total_records > 0:
                global_throughput = total_records / elapsed if elapsed > 0 else 0
                context.log.info(f"✅ Extraction et chargement terminés en {elapsed:.2f}s")
                context.log.info(f"📊 Total: {total_records:,} records en {page_count} pages")
                context.log.info(f"⚡ Performance globale: {global_throughput:.0f} records/seconde")
            else:
                context.log.warning(f"⚠️  Table {table_name} vide après ingestion ! Vérifier: API Hub'Eau, filtres date, ou réseau.")

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
