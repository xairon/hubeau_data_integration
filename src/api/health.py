"""
Health checks simplifiés pour les services de l'infrastructure
"""
import os
from typing import Dict


def check_postgres() -> bool:
    """
    Check simple PostgreSQL.

    Returns:
        True si PostgreSQL est accessible, False sinon
    """
    try:
        import psycopg

        # Connection string depuis les variables d'environnement
        conn_string = (
            f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
            f"port={os.getenv('POSTGRES_PORT', '5432')} "
            f"dbname={os.getenv('POSTGRES_DB', 'hubeau')} "
            f"user={os.getenv('POSTGRES_USER', 'hubeau')} "
            f"password={os.getenv('POSTGRES_PASSWORD', '')}"
        )

        with psycopg.connect(conn_string, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()

        return True

    except Exception:
        return False


def check_hubeau_api() -> bool:
    """
    Check simple Hub'Eau API.

    Returns:
        True si l'API Hub'Eau est accessible, False sinon
    """
    try:
        import httpx

        # Test sur un endpoint simple et rapide
        test_url = "https://hubeau.eaufrance.fr/api/v1/etat_piscicole/stations"

        with httpx.Client(timeout=10) as client:
            response = client.get(test_url, params={"size": 1})
            response.raise_for_status()
            return True

    except Exception:
        return False


def health_check() -> Dict[str, bool]:
    """
    Health check global simplifié.

    Returns:
        Dict avec le status de chaque service

    Example:
        >>> health = health_check()
        >>> if not health["postgres"]:
        ...     print("PostgreSQL is down!")
    """
    return {
        "postgres": check_postgres(),
        "hubeau_api": check_hubeau_api()
    }


def is_healthy() -> bool:
    """
    Vérifie si tous les services critiques sont healthy.

    Returns:
        True si tous les services sont UP, False sinon
    """
    health = health_check()

    # Services critiques (sans hubeau_api qui est externe)
    critical_services = ["postgres"]

    return all(health.get(service, False) for service in critical_services)
