"""
Utilitaire pour charger automatiquement les schémas SQL des tables Hub'Eau
"""

from pathlib import Path
import psycopg2
import logging

logger = logging.getLogger(__name__)


def get_schema_sql_path(table_name: str) -> Path:
    """
    Retourne le chemin vers le fichier SQL de schéma d'une table

    Args:
        table_name: Nom de la table (ex: "quality_groundwater_stations")

    Returns:
        Path vers scripts/schema/<table_name>.sql
    """
    import os

    # Priorité 1: Variable d'environnement HUBEAU_SCHEMA_DIR
    schema_dir = os.getenv("HUBEAU_SCHEMA_DIR")
    if schema_dir:
        schema_path = Path(schema_dir) / f"{table_name}.sql"
        if schema_path.exists():
            return schema_path

    # Priorité 2: /app/scripts/schema (Docker mount point)
    docker_path = Path("/app/scripts/schema") / f"{table_name}.sql"
    if docker_path.exists():
        return docker_path

    # Priorité 3: Relatif au working directory (dev local)
    relative_path = Path("scripts/schema") / f"{table_name}.sql"
    if relative_path.exists():
        return relative_path

    # Fallback: retourner le chemin Docker (pour générer l'erreur appropriée)
    return docker_path


def ensure_table_exists(table_name: str, conn_params: dict) -> None:
    """
    S'assure que la table existe, sinon la crée via son script SQL

    Utilisé par les assets Dagster pour créer automatiquement leur table
    avant l'ingestion.

    Args:
        table_name: Nom de la table
        conn_params: Paramètres de connexion PostgreSQL
                    {host, port, database, user, password}

    Raises:
        RuntimeError: Si la création échoue
    """
    try:
        # Vérifier si la table existe
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'staging'
                AND table_name = %s
            )
        """, (table_name,))
        exists = cursor.fetchone()[0]
        cursor.close()

        if exists:
            logger.debug(f"✓ Table staging.{table_name} existe déjà")
            conn.close()
            return

        # Table n'existe pas - charger le schéma SQL
        logger.warning(f"⚠️  Table staging.{table_name} n'existe pas - création automatique")

        schema_path = get_schema_sql_path(table_name)

        if not schema_path.exists():
            conn.close()
            raise RuntimeError(
                f"Fichier SQL manquant: {schema_path}\n"
                f"Les schémas SQL doivent être dans scripts/schema/"
            )

        # Lire et exécuter le script SQL
        with open(schema_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        cursor = conn.cursor()

        # Séparer les statements SQL et les exécuter un par un
        # (psycopg2 ne peut pas exécuter plusieurs statements avec execute())
        statements = []
        current_statement = []

        for line in sql_content.split('\n'):
            stripped = line.strip()
            # Skip comments
            if stripped.startswith('--'):
                continue
            if stripped:
                current_statement.append(line)
            # Check if statement is complete (ends with semicolon)
            if stripped.endswith(';'):
                statement = '\n'.join(current_statement)
                if statement.strip():
                    statements.append(statement)
                current_statement = []

        # Execute each statement
        for statement in statements:
            cursor.execute(statement)

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"✅ Schéma chargé: staging.{table_name}")

    except psycopg2.Error as e:
        logger.error(f"❌ Erreur SQL lors de la création de {table_name}: {e}")
        raise RuntimeError(f"Impossible de créer la table {table_name}") from e
    except Exception as e:
        logger.error(f"❌ Erreur lors de la création de {table_name}: {e}")
        raise RuntimeError(f"Impossible de créer la table {table_name}") from e
