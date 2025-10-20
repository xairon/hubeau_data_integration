"""
Destinations DLT configurées pour Hub'Eau
"""
import dlt
import os
from typing import Dict, Optional, Any


def get_destination(dest_type: str, config: Dict = None):
    """
    Factory pour créer les destinations DLT.

    Args:
        dest_type: Type de destination (postgres, duckdb, bigquery, snowflake)
        config: Configuration spécifique (optionnel)

    Returns:
        Destination DLT configurée

    Examples:
        # PostgreSQL par défaut
        dest = get_destination("postgres")

        # DuckDB pour tests
        dest = get_destination("duckdb", {"path": "test.db"})
    """

    if dest_type == "postgres":
        return get_postgres_destination(config)
    elif dest_type == "duckdb":
        return get_duckdb_destination(config)
    elif dest_type == "bigquery":
        return get_bigquery_destination(config)
    elif dest_type == "snowflake":
        return get_snowflake_destination(config)
    else:
        raise ValueError(f"Unknown destination type: {dest_type}")


def get_postgres_destination(config: Dict = None) -> Any:
    """
    Destination PostgreSQL avec support PostGIS.

    Args:
        config: Configuration optionnelle

    Returns:
        Destination PostgreSQL configurée

    Notes:
        - Support natif PostGIS pour les données géospatiales
        - Utilise les variables d'environnement PG_* si présentes
        - Sinon utilise dlt.secrets
    """
    config = config or {}

    # Essayer d'abord les secrets DLT
    try:
        credentials = dlt.secrets.get("destination.postgres.credentials")
    except:
        credentials = None

    # Fallback sur variables d'environnement
    if not credentials:
        credentials = {
            "database": os.getenv("PG_DB", config.get("database", "postgres")),
            "username": os.getenv("PG_USER", config.get("username", "postgres")),
            "password": os.getenv("PG_PASSWORD", config.get("password", "postgres")),
            "host": os.getenv("PG_HOST", config.get("host", "postgres")),
            "port": int(os.getenv("PG_PORT", config.get("port", 5432)))
        }

    # Support PostGIS
    if config.get("enable_postgis", True):
        # Ajouter l'extension PostGIS si nécessaire
        credentials["init_commands"] = [
            "CREATE EXTENSION IF NOT EXISTS postgis;",
            "CREATE EXTENSION IF NOT EXISTS postgis_topology;"
        ]

    # Créer la destination
    # Forcer le schéma hubeau pour utiliser les tables pré-définies
    dataset_name = config.get("dataset_name", os.getenv("HUBEAU_SCHEMA", "hubeau"))

    return dlt.destinations.postgres(
        credentials=credentials,
        dataset_name=dataset_name,
        # Désactiver la création automatique de tables par DLT
        replace_strategy="truncate-and-insert",  # Utiliser les tables existantes
        **{k: v for k, v in config.items() if k not in ["credentials", "dataset_name", "enable_postgis"]}
    )


def get_duckdb_destination(config: Dict = None) -> Any:
    """
    Destination DuckDB (pour dev/test local).

    Args:
        config: Configuration optionnelle

    Returns:
        Destination DuckDB configurée

    Notes:
        - Parfait pour développement et tests
        - Base de données embarquée, pas de serveur nécessaire
        - Support SQL complet et performances excellentes
        - Export facile vers parquet
    """
    config = config or {}

    # Chemin de la base de données
    db_path = config.get("path", os.getenv("DUCKDB_PATH", "data/hubeau.duckdb"))

    # Créer le répertoire si nécessaire
    from pathlib import Path
    db_dir = Path(db_path).parent
    db_dir.mkdir(parents=True, exist_ok=True)

    return dlt.destinations.duckdb(
        credentials=db_path,
        **{k: v for k, v in config.items() if k != "path"}
    )


def get_bigquery_destination(config: Dict = None) -> Any:
    """
    Destination Google BigQuery.

    Args:
        config: Configuration optionnelle

    Returns:
        Destination BigQuery configurée
    """
    config = config or {}

    credentials = {
        "project_id": os.getenv("GCP_PROJECT_ID", config.get("project_id")),
        "location": os.getenv("GCP_LOCATION", config.get("location", "EU"))
    }

    # Credentials file ou service account
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        credentials["credentials"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    dataset_name = config.get("dataset_name", "hubeau_raw")  # Raw layer

    return dlt.destinations.bigquery(
        credentials=credentials,
        dataset_name=dataset_name,
        **{k: v for k, v in config.items() if k not in ["credentials", "dataset_name", "project_id", "location"]}
    )


def get_snowflake_destination(config: Dict = None) -> Any:
    """
    Destination Snowflake.

    Args:
        config: Configuration optionnelle

    Returns:
        Destination Snowflake configurée
    """
    config = config or {}

    credentials = {
        "username": os.getenv("SNOWFLAKE_USER", config.get("username")),
        "password": os.getenv("SNOWFLAKE_PASSWORD", config.get("password")),
        "account": os.getenv("SNOWFLAKE_ACCOUNT", config.get("account")),
        "database": os.getenv("SNOWFLAKE_DATABASE", config.get("database", "HUBEAU")),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", config.get("warehouse", "COMPUTE_WH")),
        "role": os.getenv("SNOWFLAKE_ROLE", config.get("role"))
    }

    schema_name = config.get("schema_name", "RAW")  # Raw layer - uppercase for Snowflake

    return dlt.destinations.snowflake(
        credentials=credentials,
        dataset_name=schema_name,
        **{k: v for k, v in config.items() if k not in ["credentials", "schema_name"]}
    )


def create_multi_destination(destinations: list) -> Any:
    """
    Crée une destination multiple pour charger vers plusieurs destinations.

    Args:
        destinations: Liste de tuples (type, config)

    Returns:
        Liste de destinations configurées

    Example:
        # Charger vers PostgreSQL ET DuckDB
        dests = create_multi_destination([
            ("postgres", {"dataset_name": "raw"}),
            ("duckdb", {"path": "backup.duckdb"})
        ])
    """
    configured_dests = []

    for dest_type, dest_config in destinations:
        dest = get_destination(dest_type, dest_config)
        configured_dests.append(dest)

    return configured_dests


def get_destination_from_config(yaml_config: Dict) -> Any:
    """
    Crée une destination depuis la configuration YAML.

    Args:
        yaml_config: Configuration YAML complète

    Returns:
        Destination configurée selon la config

    Notes:
        Utilise la première destination définie dans le YAML
        ou postgres par défaut
    """
    destinations_config = yaml_config.get("destinations", {})

    if not destinations_config:
        # Par défaut PostgreSQL
        return get_postgres_destination()

    # Prendre la première destination définie
    for dest_type, dest_config in destinations_config.items():
        return get_destination(dest_type, dest_config)

    # Fallback
    return get_postgres_destination()