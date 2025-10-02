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


@asset(required_resource_keys={"s3"})
def hydrobio_taxons(context: AssetExecutionContext) -> Dict[str, Any]:
    return ingest_dlt(context, "configs/hubeau/hydrobio_taxons.yml")
