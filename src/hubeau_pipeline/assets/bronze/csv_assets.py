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
from dagster import asset, AssetExecutionContext, Output, MetadataValue, Config
from pydantic import Field
from typing import Optional, Literal, Dict, Any

from hubeau_pipeline.sources.hubeau_csv_source import hubeau_csv_source, IngestionMode
from src.dlt_pipeline.destinations import get_postgres_destination


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

    @asset(
        name=asset_name,
        group_name=group_name,
        compute_kind="dlt",
        op_tags={"format": "csv", "source": "hubeau"}
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

        # Execution avec configuration optimisée mémoire
        start_time = time.time()

        context.log.info(
            f"⚙️  Configuration DLT: workers=1 (économie mémoire), "
            f"format temporaire=jsonl (streaming optimisé)"
        )

        try:
            # Exécuter avec 1 seul worker pour économiser mémoire
            #
            # Note: loader_file_format contrôle le format des fichiers TEMPORAIRES
            # créés par DLT entre NORMALIZE et LOAD (dans /tmp/dlt_pipelines/)
            # - Ces fichiers sont créés APRÈS téléchargement des CSV
            # - Ils sont utilisés pour l'insertion en PostgreSQL
            # - Ils sont automatiquement supprimés après l'import
            #
            # JSONL = moins de RAM que Parquet car DLT stream ligne par ligne
            load_info = pipeline.run(
                source,
                workers=1,                      # 1 worker = moins de RAM utilisée
                loader_file_format="jsonl"      # Format temporaire optimisé mémoire
            )
            elapsed = time.time() - start_time

            # Extraire metriques de load_info
            rows_loaded = 0

            # Methode 1: Via load_packages
            if hasattr(load_info, 'load_packages') and load_info.load_packages:
                for package in load_info.load_packages:
                    if hasattr(package, 'jobs') and package.jobs:
                        for job in package.jobs:
                            if hasattr(job, '_rows_count'):
                                rows_loaded += job._rows_count
                            elif hasattr(job, 'job_file_info'):
                                # Compter les lignes dans le fichier
                                job_info = job.job_file_info
                                if hasattr(job_info, 'rows_count'):
                                    rows_loaded += job_info.rows_count

            # Methode 2: Via metrics si disponible
            if rows_loaded == 0 and hasattr(load_info, 'metrics'):
                metrics = load_info.metrics
                if hasattr(metrics, 'rows_count'):
                    rows_loaded = metrics.rows_count

            # Methode 3: Via first_run si disponible
            if rows_loaded == 0 and hasattr(load_info, 'first_run') and load_info.first_run:
                try:
                    rows_loaded = sum(
                        job._rows_count if hasattr(job, '_rows_count') else 0
                        for package in load_info.first_run.load_packages
                        for job in (package.jobs if hasattr(package, 'jobs') else [])
                    )
                except:
                    pass

            # Log détaillé des résultats
            throughput = rows_loaded / elapsed if elapsed > 0 else 0
            context.log.info(
                f"✅ Termine: {rows_loaded:,} lignes en {elapsed:.2f}s "
                f"({throughput:.1f} lignes/sec)"
            )

            # Debug: afficher structure load_info si rows_loaded == 0
            if rows_loaded == 0:
                context.log.warning(f"⚠️  Aucune ligne detectee dans load_info")
                context.log.debug(f"load_info type: {type(load_info)}")
                context.log.debug(f"load_info attributes: {dir(load_info)}")

                # Afficher les packages disponibles pour debug
                if hasattr(load_info, 'load_packages'):
                    context.log.debug(f"Nombre de load_packages: {len(load_info.load_packages)}")

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
