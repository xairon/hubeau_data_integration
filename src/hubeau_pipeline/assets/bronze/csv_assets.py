"""
Assets Dagster pour ingestion CSV Hub'Eau

Factory pattern pour generer automatiquement 22 assets avec support multi-mode :
- FULL : Tout l'historique
- YEAR : Une annee specifique
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
from src.dlt_pipeline.destinations import get_postgres_destination


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


def count_rows_in_postgres(table_name: str, schema: str = "hubeau") -> int:
    """
    Compte les lignes dans une table PostgreSQL.

    Utilisé pour obtenir le vrai nombre de lignes chargées,
    car DLT load_info ne retourne pas toujours les métriques correctement.

    Args:
        table_name: Nom de la table
        schema: Schéma PostgreSQL (défaut: hubeau)

    Returns:
        Nombre de lignes dans la table, ou 0 si erreur/table inexistante
    """
    import psycopg2

    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST", "postgres"),
            port=os.getenv("PG_PORT", "5432"),
            database=os.getenv("PG_DB", "postgres"),
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PASSWORD")
        )

        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
            count = cur.fetchone()[0]

        conn.close()
        return count

    except Exception as e:
        # Si erreur (table n'existe pas encore, connexion échouée, etc.)
        return 0


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
        freshness_policy=CHRONIQUES_FRESHNESS_POLICY if supports_date_filter else None,  # ← Freshness 48h
        metadata={
            "partition_type": "time_based" if supports_date_filter else "none",
            "supports_incremental": supports_date_filter,
            "description": f"Ingestion Hub'Eau pour {resource_name}",
            "depends_on": ASSET_DEPENDENCIES.get(resource_name, []),  # ← Metadata dependencies
            "freshness_check": "48h" if supports_date_filter else "none"  # ← Metadata freshness
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
        if context.has_partition_key:
            partition = context.partition_key
            context.log.info(f"📋 Partition sélectionnée: {partition}")

            if partition == "full":
                config.mode = "full"
                config.year = None
            elif partition == "incremental":
                config.mode = "incremental"
                config.year = None
                # Garder incremental_days de la config (défaut: 2)
            else:
                # C'est une année (2024, 2023, etc.)
                try:
                    year_value = int(partition)
                    config.mode = "year"
                    config.year = year_value
                except ValueError:
                    context.log.error(f"❌ Partition invalide: {partition} (attendu: full, incremental, ou YYYY)")
                    raise ValueError(f"Partition invalide: {partition}")

        # Validation
        if config.mode == "year" and not config.year:
            raise ValueError("Mode YEAR necessite le parametre 'year'")

        if not supports_date_filter and config.mode in ["year", "incremental"]:
            context.log.warning(
                f"{resource_name} ne supporte pas les filtres date. "
                f"Passage en mode FULL."
            )
            config.mode = "full"

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

        # Execution
        start_time = time.time()

        try:
            # Exécuter le pipeline DLT
            # DLT va :
            # 1. Recevoir les données depuis la source (Pandas a déjà inféré les types)
            # 2. Créer les tables PostgreSQL automatiquement si elles n'existent pas
            # 3. Insérer les données avec UPSERT (merge) basé sur primary_key
            load_info = pipeline.run(source)
            elapsed = time.time() - start_time

            # ✅ FIX: Compter les lignes directement dans PostgreSQL
            # car DLT load_info ne retourne pas toujours les métriques correctement
            table_name = yaml_config['resource']['name']
            context.log.info(f"🔍 Vérification des lignes chargées dans PostgreSQL...")

            rows_loaded = count_rows_in_postgres(table_name)

            # Log détaillé des résultats
            throughput = rows_loaded / elapsed if elapsed > 0 else 0
            context.log.info(
                f"✅ Terminé: {rows_loaded:,} lignes en base PostgreSQL "
                f"(durée: {elapsed:.2f}s, débit: {throughput:.1f} lignes/sec)"
            )

            # Alerter si table vide après ingestion
            if rows_loaded == 0:
                context.log.warning(
                    f"⚠️  Table {table_name} vide après ingestion ! "
                    f"Vérifier: API Hub'Eau, filtres date, ou réseau."
                )

            return Output(
                value=load_info,
                metadata={
                    "mode": MetadataValue.text(config.mode),
                    "year": MetadataValue.int(config.year) if config.year else None,
                    "incremental_days": MetadataValue.int(config.incremental_days) if config.mode == "incremental" else None,
                    "rows_loaded": MetadataValue.int(rows_loaded),
                    "duration_seconds": MetadataValue.float(round(elapsed, 2)),
                    "throughput_rows_per_sec": MetadataValue.float(round(rows_loaded/elapsed, 2)) if elapsed > 0 else None
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
