"""
Utilitaires pour la connexion à la base de données PostgreSQL.
"""

import os
from typing import Optional


def get_postgres_connection_string(
    database: Optional[str] = None,
    schema: Optional[str] = None
) -> str:
    """
    Construit la chaîne de connexion PostgreSQL à partir des variables d'environnement.

    Args:
        database: Nom de la base de données (par défaut: PG_DB ou 'postgres')
        schema: Nom du schéma (optionnel)

    Returns:
        Chaîne de connexion PostgreSQL au format SQLAlchemy

    Example:
        >>> get_postgres_connection_string()
        'postgresql://user:pass@localhost:5432/postgres'
    """
    # Récupération des variables d'environnement
    host = os.getenv("PG_HOST", "postgres")
    port = os.getenv("PG_PORT", "5432")
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "")
    db = database or os.getenv("PG_DB", "postgres")

    # Construction de la chaîne de connexion
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db}"

    # Ajout du schéma si spécifié
    if schema:
        connection_string += f"?options=-csearch_path%3D{schema}"

    return connection_string


def get_postgres_connection_params() -> dict:
    """
    Retourne les paramètres de connexion PostgreSQL sous forme de dictionnaire.

    Returns:
        Dict avec les paramètres de connexion

    Example:
        >>> get_postgres_connection_params()
        {'host': 'localhost', 'port': 5432, 'database': 'postgres', ...}
    """
    return {
        'host': os.getenv("PG_HOST", "postgres"),
        'port': int(os.getenv("PG_PORT", "5432")),
        'database': os.getenv("PG_DB", "postgres"),
        'user': os.getenv("PG_USER", "postgres"),
        'password': os.getenv("PG_PASSWORD", "")
    }


def test_connection() -> bool:
    """
    Teste la connexion à PostgreSQL.

    Returns:
        True si la connexion est réussie, False sinon
    """
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(get_postgres_connection_string())
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except Exception as e:
        print(f"Erreur de connexion : {e}")
        return False