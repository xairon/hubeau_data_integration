"""Dagster resources for external dependencies used by the Hub'Eau pipeline."""

import logging
import os
from typing import Any, Dict

import boto3
import httpx
import psycopg
from dagster import resource


logger = logging.getLogger(__name__)


def _build_pg_dsn() -> str:
    """Compose the PostgreSQL DSN from environment variables."""
    password = os.getenv("PG_PASSWORD")
    if not password:
        logger.warning("⚠️  PG_PASSWORD not set; using default password for local development")
        password = "postgres"
    else:
        logger.info("✅ PG_PASSWORD loaded from environment")

    user = os.getenv("PG_USER", "postgres")
    host = os.getenv("PG_HOST", "timescaledb")
    port = os.getenv("PG_PORT", "5432")
    database = os.getenv("PG_DATABASE", "water")
    logger.info(f"🔗 PostgreSQL DSN: postgresql://{user}:***@{host}:{port}/{database}")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def _build_s3_config() -> Dict[str, str]:
    """Return the default MinIO/S3 configuration from environment variables."""
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_USER")
    secret_key = os.getenv("MINIO_PASS")
    bucket = os.getenv("MINIO_BRONZE_BUCKET", "bronze")

    if not access_key or not secret_key:
        logger.warning("⚠️  MINIO credentials not set; using default development credentials")
        logger.warning(f"   MINIO_USER={'NOT SET' if not access_key else 'SET'}")
        logger.warning(f"   MINIO_PASS={'NOT SET' if not secret_key else 'SET'}")
        access_key = access_key or "admin"
        secret_key = secret_key or "admin123"
    else:
        logger.info("✅ MINIO credentials loaded from environment")
        logger.info(f"   MINIO_USER: {access_key}")
        logger.info(f"   MINIO_ENDPOINT: {endpoint}")
        logger.info(f"   MINIO_BRONZE_BUCKET: {bucket}")

    return {
        "endpoint_url": endpoint,
        "access_key": access_key,
        "secret_key": secret_key,
        "bucket": bucket,
    }


@resource
def http_client(_):
    """Client HTTP pour les appels API Hub'Eau"""
    return httpx.Client(
        timeout=30.0, 
        headers={"User-Agent": "hubeau-pipeline/1.0"},
        follow_redirects=True
    )


@resource(config_schema={"dsn": str})
def pg_conn(init_context):
    """Connexion PostgreSQL/TimescaleDB optimisée"""
    dsn = init_context.resource_config["dsn"]
    return psycopg.connect(dsn, autocommit=True)


@resource(
    config_schema={
        "endpoint_url": str,
        "access_key": str,
        "secret_key": str,
        "bucket": str,
    }
)
def s3_client(init_context):
    """Client S3/MinIO pour le data lake"""
    config = init_context.resource_config
    client = boto3.client(
        "s3",
        endpoint_url=config["endpoint_url"],
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name="us-east-1"  # MinIO nécessite une région
    )
    return {
        "bucket": config["bucket"],
        "client": client
    }


# Configuration des ressources avec variables d'environnement
RESOURCES = {
    "http_client": http_client,
    "pg": pg_conn.configured({"dsn": _build_pg_dsn()}),
    "s3": s3_client.configured(_build_s3_config()),
}
