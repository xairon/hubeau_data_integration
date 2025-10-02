"""Dagster assets orchestrating dlt based pipelines."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import yaml
from dagster import AssetExecutionContext, MetadataValue, asset

from pipelines.dlt.hubeau_generic import run_pipeline


def _build_credentials() -> Dict[str, str]:
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    region = os.getenv("MINIO_REGION", "us-east-1")
    access_key = os.getenv("MINIO_USER")
    secret_key = os.getenv("MINIO_PASS")
    creds = {
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "endpoint_url": endpoint,
        "region_name": region,
    }
    return {k: v for k, v in creds.items() if v}


def _build_state_fs_options(credentials: Dict[str, str]) -> Dict[str, Any]:
    endpoint = credentials.get("endpoint_url")
    region = credentials.get("region_name")
    fs_options: Dict[str, Any] = {
        "key": credentials.get("aws_access_key_id"),
        "secret": credentials.get("aws_secret_access_key"),
    }
    client_kwargs: Dict[str, Any] = {}
    if endpoint:
        client_kwargs["endpoint_url"] = endpoint
    if region:
        client_kwargs["region_name"] = region
    if client_kwargs:
        fs_options["client_kwargs"] = client_kwargs
    return {k: v for k, v in fs_options.items() if v}


@asset(io_manager_key="minio_io_manager", compute_kind="python", required_resource_keys={"s3"})
def ingest_dlt(context: AssetExecutionContext, config_path: str) -> Dict[str, Any]:
    cfg = yaml.safe_load(Path(config_path).read_text())
    context.log.info("Launching dlt pipeline for %s", cfg["name"])

    minio_bucket = getattr(context.resources, "s3", {}).get("bucket")
    bucket_url = f"s3://{minio_bucket}" if minio_bucket else None
    credentials = _build_credentials()
    fs_options = _build_state_fs_options(credentials)

    if not credentials.get("aws_access_key_id") or not credentials.get("aws_secret_access_key"):
        context.log.warning(
            "Missing explicit MinIO credentials in environment; relying on default client configuration"
        )

    cfg.setdefault("state_store", f"{bucket_url.rstrip('/')}/_state" if bucket_url else None)

    load_info = run_pipeline(
        cfg,
        bucket_url=bucket_url,
        credentials=credentials,
        dataset_name=cfg.get("dataset_name"),
        file_format=cfg.get("file_format", "parquet"),
        layout=cfg.get("layout"),
        state_fs_options=fs_options,
    )
    row_count = 0
    if hasattr(load_info, "metrics"):
        row_count = load_info.metrics.row_counts.get(cfg["name"], 0)
    context.add_output_metadata(
        {
            "stream": MetadataValue.text(cfg["name"]),
            "rows": MetadataValue.int(row_count),
            "destination": MetadataValue.text("bronze"),
        }
    )
    return {"stream": cfg["name"], "rows": row_count}


# ====================================
# ASSETS HYDROBIOLOGIE
# ====================================

@asset(required_resource_keys={"s3"})
def hydrobio_taxons(context: AssetExecutionContext) -> Dict[str, Any]:
    """🐟 Hydrobiologie - Taxons biologiques"""
    return ingest_dlt(context, "configs/hubeau/hydrobio_taxons.yml")


@asset(required_resource_keys={"s3"})
def hydrobio_indices(context: AssetExecutionContext) -> Dict[str, Any]:
    """🐟 Hydrobiologie - Indices biologiques (IBGN, I2M2, etc.)"""
    return ingest_dlt(context, "configs/hubeau/hydrobio_indices.yml")


# ====================================
# ASSETS HYDROMÉTRIE
# ====================================

@asset(required_resource_keys={"s3"})
def hydrometry_observations(context: AssetExecutionContext) -> Dict[str, Any]:
    """🌊 Hydrométrie - Observations temps réel (30 derniers jours)"""
    return ingest_dlt(context, "configs/hubeau/hydrometry_observations.yml")


# ====================================
# ASSETS PIÉZOMÉTRIE
# ====================================

@asset(required_resource_keys={"s3"})
def piezometry_chroniques(context: AssetExecutionContext) -> Dict[str, Any]:
    """🕳️ Piézométrie - Chroniques de niveaux des nappes"""
    return ingest_dlt(context, "configs/hubeau/piezometry_chroniques.yml")


# ====================================
# ASSETS QUALITÉ DES EAUX
# ====================================

@asset(required_resource_keys={"s3"})
def quality_rivers_analyses(context: AssetExecutionContext) -> Dict[str, Any]:
    """🏞️ Qualité Cours d'Eau - Analyses physico-chimiques"""
    return ingest_dlt(context, "configs/hubeau/quality_rivers_analyses.yml")


@asset(required_resource_keys={"s3"})
def quality_groundwater_analyses(context: AssetExecutionContext) -> Dict[str, Any]:
    """💧 Qualité Nappes - Analyses eaux souterraines"""
    return ingest_dlt(context, "configs/hubeau/quality_groundwater_analyses.yml")


# ====================================
# ASSETS ÉCOULEMENT
# ====================================

@asset(required_resource_keys={"s3"})
def ecoulement_observations(context: AssetExecutionContext) -> Dict[str, Any]:
    """🌊 Écoulement - Observations ONDE (Observatoire National Des Étiages)"""
    return ingest_dlt(context, "configs/hubeau/ecoulement_observations.yml")


# ====================================
# ASSETS PRÉLÈVEMENTS
# ====================================

@asset(required_resource_keys={"s3"})
def prelevements_chroniques(context: AssetExecutionContext) -> Dict[str, Any]:
    """💧 Prélèvements - Chroniques de prélèvement (limite 20k stricte)"""
    return ingest_dlt(context, "configs/hubeau/prelevements_chroniques.yml")


# ====================================
# ASSETS TEMPÉRATURE
# ====================================

@asset(required_resource_keys={"s3"})
def temperature_chroniques(context: AssetExecutionContext) -> Dict[str, Any]:
    """🌡️ Température - Chroniques de température (station×mois systématique)"""
    return ingest_dlt(context, "configs/hubeau/temperature_chroniques.yml")


# ====================================
# EXPORT DES ASSETS
# ====================================

__all__ = [
    "ingest_dlt",
    # Hydrobiologie
    "hydrobio_taxons",
    "hydrobio_indices",
    # Hydrométrie
    "hydrometry_observations",
    # Piézométrie
    "piezometry_chroniques",
    # Qualité
    "quality_rivers_analyses",
    "quality_groundwater_analyses",
    # Écoulement
    "ecoulement_observations",
    # Prélèvements
    "prelevements_chroniques",
    # Température
    "temperature_chroniques",
]
