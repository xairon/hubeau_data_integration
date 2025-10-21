"""
Helpers PostgreSQL pour stratégies d'UPSERT et vérifications
"""

import os
import psycopg2
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class PostgresHelper:
    """Utilities pour interagir avec PostgreSQL"""

    @staticmethod
    def _get_connection():
        """Créer une connexion PostgreSQL"""
        return psycopg2.connect(
            host=os.getenv("PG_HOST", "postgres"),
            port=int(os.getenv("PG_PORT", "5432")),
            database=os.getenv("PG_DB", "postgres"),
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PASSWORD")
        )

    @staticmethod
    def get_table_count(table_name: str, schema: str = "hubeau") -> int:
        """Compte les records dans une table"""
        conn = PostgresHelper._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"❌ Erreur get_table_count({table_name}): {e}")
            return 0
        finally:
            conn.close()

    @staticmethod
    def get_max_date(
        table_name: str,
        date_column: str,
        schema: str = "hubeau"
    ) -> Optional[str]:
        """Récupère la date max pour chargement incrémental"""
        conn = PostgresHelper._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT MAX({date_column}) FROM {schema}.{table_name}"
                )
                result = cursor.fetchone()[0]
                if result:
                    # Convertir en ISO format si c'est un datetime
                    if hasattr(result, 'isoformat'):
                        return result.isoformat()
                    return str(result)
                return None
        except Exception as e:
            logger.warning(f"⚠️ Impossible de récupérer MAX({date_column}) de {table_name}: {e}")
            return None
        finally:
            conn.close()

    @staticmethod
    def table_exists(table_name: str, schema: str = "hubeau") -> bool:
        """Vérifie si une table existe"""
        conn = PostgresHelper._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = %s
                    )
                    """,
                    (schema, table_name)
                )
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"❌ Erreur table_exists({table_name}): {e}")
            return False
        finally:
            conn.close()

    @staticmethod
    def should_refresh_reference_table(
        table_name: str,
        api_count: int,
        schema: str = "hubeau",
        tolerance: int = 10
    ) -> bool:
        """
        Détermine si une table de référence doit être rafraîchie

        Args:
            table_name: Nom de la table
            api_count: Nombre de records dans l'API
            schema: Nom du schéma
            tolerance: Tolérance de différence (nombre de records)

        Returns:
            True si refresh nécessaire
        """
        # Vérifier si la table existe
        if not PostgresHelper.table_exists(table_name, schema):
            logger.info(f"📊 {table_name}: Table n'existe pas → chargement initial requis")
            return True

        db_count = PostgresHelper.get_table_count(table_name, schema)

        if db_count == 0:
            logger.info(f"📊 {table_name}: Table vide → chargement initial requis")
            return True

        diff = abs(db_count - api_count)
        if diff > tolerance:
            logger.info(f"🔄 {table_name}: API={api_count}, DB={db_count} (diff={diff}) → refresh requis")
            return True

        logger.info(f"✅ {table_name}: À jour ({db_count} records, diff={diff})")
        return False
