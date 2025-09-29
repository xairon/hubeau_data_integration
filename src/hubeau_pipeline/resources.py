"""
Ressources Dagster pour l'intégration Hub'Eau
Connexions aux bases de données et services externes
"""

import boto3
import psycopg
import httpx
from neo4j import GraphDatabase
from dagster import resource
# from dagster._utils import merge_dicts  # Non disponible dans cette version
from typing import Dict, Any


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


@resource(config_schema={"dsn": str, "user": str, "password": str})
def neo4j_driver(init_context):
    """Driver Neo4j pour le graphe sémantique"""
    uri = init_context.resource_config["dsn"]
    auth = (
        init_context.resource_config.get("user", "neo4j"),
        init_context.resource_config.get("password")
    )
    return GraphDatabase.driver(
        uri, 
        auth=auth, 
        max_connection_pool_size=10,
        connection_timeout=30
    )


@resource(config_schema={
    "endpoint_url": str, 
    "access_key": str, 
    "secret_key": str, 
    "bucket": str
})
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
    "pg": pg_conn.configured({
        "dsn": "postgresql://postgres:{{ env.PG_PASSWORD }}@timescaledb:5432/water"
    }),
    "neo4j": neo4j_driver.configured({
        "dsn": "bolt://neo4j:7687",
        "user": "neo4j",
        "password": "{{ env.NEO4J_PASSWORD }}"
    }),
    "s3": s3_client.configured({
        "endpoint_url": "http://minio:9000",
        "access_key": "{{ env.MINIO_USER }}",
        "secret_key": "{{ env.MINIO_PASS }}",
        "bucket": "bronze"
    }),
}
