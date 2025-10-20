"""Dagster resources for external dependencies used by the Hub'Eau pipeline."""

import logging
import os
from typing import Any, Dict

import httpx
import psycopg
from dagster import resource


logger = logging.getLogger(__name__)


def _build_pg_dsn() -> str:
    """Compose the PostgreSQL DSN from environment variables."""
    password = os.getenv("PG_PASSWORD")

    # ✅ FAIL FAST: Never use default password in production
    if not password:
        error_msg = (
            "❌ CRITICAL: PG_PASSWORD not set!\n"
            "This MUST be defined in environment variables (GitLab CI/CD Variables)."
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info("✅ PG_PASSWORD loaded from environment")

    user = os.getenv("PG_USER", "postgres")
    host = os.getenv("PG_HOST", "timescaledb")
    port = os.getenv("PG_PORT", "5432")
    database = os.getenv("PG_DATABASE", "water")
    logger.info(f"🔗 PostgreSQL DSN: postgresql://{user}:***@{host}:{port}/{database}")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


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


# Configuration des ressources avec variables d'environnement
RESOURCES = {
    "http_client": http_client,
    "pg": pg_conn.configured({"dsn": _build_pg_dsn()}),
}
