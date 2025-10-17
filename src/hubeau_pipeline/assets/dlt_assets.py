"""Dagster assets orchestrating Hub'Eau dlt pipelines."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from dagster import AssetExecutionContext, DailyPartitionsDefinition, StaticPartitionsDefinition, asset
from dlt.common.typing import TSecretValue

from dlt_pipeline.hubeau_generic import run_pipeline

# Partitions pour les données historiques (annuelles depuis 2020)
YEARLY_PARTITIONS = StaticPartitionsDefinition([str(year) for year in range(2020, 2026)])

# Partitions pour les données temps réel (30 derniers jours)
DAILY_PARTITIONS = DailyPartitionsDefinition(start_date="2022-01-01")


def _resolve_config_path(config_path: str) -> Path:
    """Return an absolute path for the provided configuration file."""

    candidate = Path(config_path)
    if candidate.is_absolute() and candidate.exists():
        return candidate

    # Dagster images mount the repository in /app, fall back to the repo root otherwise
    repo_root = Path("/app")
    resolved = repo_root / config_path.lstrip("/")
    if resolved.exists():
        return resolved

    # Default to the project root relative path
    return Path.cwd() / config_path


def _build_credentials() -> Dict[str, TSecretValue]:
    """Build MinIO/S3 credentials expected by dlt destinations."""

    minio_user = os.getenv("MINIO_USER")
    minio_pass = os.getenv("MINIO_PASS")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_region = os.getenv("MINIO_REGION", "us-east-1")

    # ✅ FAIL FAST: Validate credentials
    if not minio_user or not minio_pass:
        raise ValueError(
            f"CRITICAL: MinIO credentials not set! "
            f"MINIO_USER={'NOT SET' if not minio_user else 'SET'}, "
            f"MINIO_PASS={'NOT SET' if not minio_pass else 'SET'}"
        )

    return {
        "aws_access_key_id": TSecretValue(minio_user),
        "aws_secret_access_key": TSecretValue(minio_pass),
        "endpoint_url": minio_endpoint,
        "region_name": minio_region,
    }


def _build_state_fs_options(credentials: Dict[str, TSecretValue]) -> Dict[str, Any]:
    """Return fsspec-compatible options to persist dlt state remotely."""

    key = credentials.get("aws_access_key_id")
    secret = credentials.get("aws_secret_access_key")
    endpoint = credentials.get("endpoint_url")
    region = credentials.get("region_name")

    fs_options: Dict[str, Any] = {}
    if key:
        fs_options["key"] = str(key)
    if secret:
        fs_options["secret"] = str(secret)

    client_kwargs: Dict[str, Any] = {}
    if endpoint:
        client_kwargs["endpoint_url"] = endpoint
    if region:
        client_kwargs["region_name"] = region
    if client_kwargs:
        fs_options["client_kwargs"] = client_kwargs

    return fs_options


# ====================================
# Generic dlt Ingestion Asset
# ====================================

# ✅ FONCTION SUPPRIMÉE: extract_station_codes_from_result
# Cette fonction a été déplacée vers src/hubeau_pipeline/assets/bronze/dlt_assets.py
# avec une logique améliorée qui récupère les stations actives depuis l'API Hub'Eau

def ingest_dlt(context: AssetExecutionContext, config_path: str, stations_data: Optional[list[str]] = None) -> Dict[str, Any]:
    """Execute a dlt pipeline described by a YAML configuration file."""

    config_file = _resolve_config_path(config_path)
    context.log.info("🚀 Starting dlt ingestion for config: %s", config_file)

    with config_file.open("r", encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)

    partition_key = context.partition_key if context.has_partition_key else None
    if partition_key and cfg.get("slicer", {}).get("mode") == "datetime":
        from datetime import datetime

        context.log.info("📅 Partition: %s", partition_key)
        try:
            if len(partition_key) == 4 and partition_key.isdigit():
                year = int(partition_key)
                cfg["slicer"]["start_date"] = f"{year}-01-01"
                cfg["slicer"]["end_date"] = f"{year}-12-31"
                context.log.info("🗓️ Ingestion pour l'année %s", year)
            else:
                datetime.strptime(partition_key, "%Y-%m-%d")
                cfg["slicer"]["start_date"] = partition_key
                cfg["slicer"]["end_date"] = partition_key
                context.log.info("🗓️ Ingestion pour le jour %s", partition_key)
        except ValueError:
            context.log.warning("⚠️ Could not parse partition key: %s", partition_key)

    context.log.info("Loaded dlt config: %s", cfg["name"])

    credentials = _build_credentials()
    state_fs_options = _build_state_fs_options(credentials)
    bucket_url = "s3://bronze"

    load_info = run_pipeline(
        cfg,
        bucket_url=bucket_url,
        credentials=credentials,
        dataset_name=cfg.get("dataset_name", "bronze"),
        file_format=cfg.get("file_format", "json"),
        layout=cfg.get("layout", "{table_name}/{curr_date}/data.json"),
        state_fs_options=state_fs_options,
        stations_data=stations_data,
        partition_date=None,  # Cette version gère les partitions différemment
    )

    context.log.info("✅ dlt pipeline for %s finished.", cfg["name"])

    row_count = 0
    if hasattr(load_info, "load_packages") and load_info.load_packages:
        for package in load_info.load_packages:
            for job in getattr(package, "jobs", []):
                if getattr(job, "job_file_type", None) == "data":
                    row_count += getattr(job, "records_count", 0)

    return {"stream": cfg["name"], "rows": row_count}


# ====================================
# Hub'Eau Specific dlt Assets
# ====================================


@asset(group_name="hubeau_hydrobiology", partitions_def=YEARLY_PARTITIONS)
def hydrobio_taxons(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrobiology taxons data using dlt."""

    return ingest_dlt(context, "configs/hubeau/hydrobio_taxons.yml")


@asset(group_name="hubeau_hydrobiology", partitions_def=YEARLY_PARTITIONS)
def hydrobio_indices(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrobiology indices data using dlt."""

    return ingest_dlt(context, "configs/hubeau/hydrobio_indices.yml")


@asset(group_name="hubeau_hydrometry")
def hydrometry_observations(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrometry observations data using dlt (30 derniers jours)."""

    return ingest_dlt(context, "configs/hubeau/hydrometry_observations.yml")


@asset(group_name="hubeau_piezometry", partitions_def=DAILY_PARTITIONS)
def piezometry_chroniques(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests piezometry chroniques data using dlt."""

    return ingest_dlt(context, "configs/hubeau/piezometry_chroniques.yml")


@asset(group_name="hubeau_quality_rivers", partitions_def=YEARLY_PARTITIONS)
def quality_rivers_analyses(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests superficial waterbodies quality analyses data using dlt."""

    return ingest_dlt(context, "configs/hubeau/quality_rivers_analyses.yml")


@asset(group_name="hubeau_quality_groundwater", partitions_def=YEARLY_PARTITIONS)
def quality_groundwater_analyses(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests groundwater quality analyses data using dlt."""

    return ingest_dlt(context, "configs/hubeau/quality_groundwater_analyses.yml")


@asset(group_name="hubeau_ecoulement", partitions_def=YEARLY_PARTITIONS)
def ecoulement_observations(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests ecoulement observations data using dlt."""

    return ingest_dlt(context, "configs/hubeau/ecoulement_observations.yml")


@asset(group_name="hubeau_prelevements", partitions_def=YEARLY_PARTITIONS)
def prelevements_chroniques(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests prelevements chroniques data using dlt."""

    return ingest_dlt(context, "configs/hubeau/prelevements_chroniques.yml")


# ====================================
# ASSETS DE STATIONS DE RÉFÉRENCE
# ====================================

@asset(group_name="hubeau_hydrometry")
def hydrometry_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrometry stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/hydrometry_stations.yml")

@asset(group_name="hubeau_piezometry")
def piezometry_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests piezometry stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/piezometry_stations.yml")

@asset(group_name="hubeau_quality_rivers")
def quality_rivers_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests quality rivers stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/quality_rivers_stations.yml")

@asset(group_name="hubeau_quality_groundwater")
def quality_groundwater_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests quality groundwater stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/quality_groundwater_stations.yml")

@asset(group_name="hubeau_ecoulement")
def ecoulement_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests ecoulement stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/ecoulement_stations.yml")

@asset(group_name="hubeau_hydrobio")
def hydrobio_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrobiology stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/hydrobio_stations.yml")

@asset(group_name="hubeau_prelevements")
def prelevements_ouvrages_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests prelevements ouvrages reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/prelevements_ouvrages.yml")

@asset(group_name="hubeau_prelevements")
def prelevements_points_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests prelevements points reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/prelevements_points.yml")

@asset(group_name="hubeau_temperature")
def temperature_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests temperature stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/temperature_stations.yml")


@asset(group_name="hubeau_temperature", partitions_def=YEARLY_PARTITIONS)
def temperature_chroniques(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests temperature chroniques data using dlt with yearly partitions and automatic fallback."""
    
    context.log.info(f"📊 Processing temperature chroniques with automatic fallback")
    
    return ingest_dlt(context, "configs/hubeau/temperature_chroniques.yml")


__all__ = [
    "ingest_dlt",
    "hydrobio_taxons",
    "hydrobio_indices",
    "hydrometry_observations",
    "piezometry_chroniques",
    "quality_rivers_analyses",
    "quality_groundwater_analyses",
    "ecoulement_observations",
    "prelevements_chroniques",
    "temperature_chroniques",
    "temperature_stations_reference",
    # Nouveaux assets de stations de référence
    "hydrometry_stations_reference",
    "piezometry_stations_reference",
    "quality_rivers_stations_reference",
    "quality_groundwater_stations_reference",
    "ecoulement_stations_reference",
    "hydrobio_stations_reference",
    "prelevements_ouvrages_reference",
    "prelevements_points_reference",
]
