"""
Extraction des codes stations depuis PostgreSQL.
Remplace station_minio.py pour architecture sans MinIO.
"""
import os
import logging
from typing import List, Dict
import psycopg

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_postgres_connection():
    """Crée une connexion PostgreSQL depuis variables d'environnement."""
    conn_params = {
        "dbname": os.getenv("PG_DB", "postgres"),
        "user": os.getenv("PG_USER", "postgres"),
        "password": os.getenv("PG_PASSWORD"),
        "host": os.getenv("PG_HOST", "postgres"),
        "port": os.getenv("PG_PORT", "5432")
    }

    if not conn_params["password"]:
        raise ValueError("PG_PASSWORD environment variable not set")

    return psycopg.connect(**conn_params)


def _get_table_and_column_for_station_type(station_type: str) -> tuple[str, str]:
    """
    Retourne (table_name, code_column_name) pour un type de station.

    Args:
        station_type: Type de station ("piezometry", "hydrometry", etc.)

    Returns:
        Tuple (table_name, code_column_name)
    """
    # Mapping type → (table, colonne_code)
    mapping = {
        "piezometry": ("piezometry_stations", "code_bss"),
        "hydrometry": ("hydrometry_stations", "code_station"),
        "quality_rivers": ("quality_rivers_stations", "code_station"),
        "quality_groundwater": ("quality_groundwater_stations", "code_station"),
        "hydrobio": ("hydrobio_stations", "code_station"),
        "ecoulement": ("ecoulement_stations", "code_station"),
        "prelevements": ("prelevements_ouvrages", "code_ouvrage"),
        "temperature": ("temperature_stations", "code_station")
    }

    return mapping.get(station_type, (f"{station_type}_stations", "code_station"))


def extract_station_codes_from_postgres(station_type: str) -> List[str]:
    """
    Extrait les codes de stations depuis PostgreSQL.
    Remplace extract_station_codes_from_minio().

    Args:
        station_type: Type de station ("piezometry", "hydrometry", etc.)

    Returns:
        Liste des codes station

    Raises:
        ValueError: Si table n'existe pas (ingestion stations requise d'abord)
    """
    table, code_column = _get_table_and_column_for_station_type(station_type)
    schema = os.getenv("HUBEAU_SCHEMA", "hubeau")

    logger.debug(f"🔍 Extraction stations depuis PostgreSQL: {schema}.{table}")

    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                # Vérifier si la table existe
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = %s
                        AND table_name = %s
                    )
                """, (schema, table))

                table_exists = cur.fetchone()[0]

                if not table_exists:
                    error_msg = (
                        f"❌ Table {schema}.{table} n'existe pas!\n"
                        f"   Vous devez d'abord exécuter l'asset '{station_type}_stations_reference'\n"
                        f"   pour ingérer les stations de référence."
                    )
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                # Extraire les codes station
                cur.execute(f"""
                    SELECT DISTINCT {code_column}
                    FROM {schema}.{table}
                    WHERE {code_column} IS NOT NULL
                    ORDER BY {code_column}
                """)

                stations = [row[0] for row in cur.fetchall()]
                logger.info(f"📂 {len(stations)} stations {station_type} trouvées dans PostgreSQL")

                if len(stations) == 0:
                    logger.warning(f"⚠️ Table {schema}.{table} est vide!")

                return stations

    except psycopg.Error as e:
        logger.error(f"❌ Erreur PostgreSQL lors de l'extraction stations: {e}")
        raise


def filter_active_stations_for_period(
    stations: List[str],
    partition_date: str,
    station_type: str
) -> List[str]:
    """
    Filtre les stations actives pour une période donnée.
    Utilise les colonnes date_debut/date_fin si disponibles.

    Args:
        stations: Liste de codes station à filtrer
        partition_date: Date partition format "YYYY-MM-DD" (ex: "2024-01-01")
        station_type: Type de station

    Returns:
        Stations actives pour la période (subset de stations)
    """
    from datetime import datetime

    if not stations:
        logger.warning("⚠️ Liste de stations vide, aucun filtrage possible")
        return []

    table, code_column = _get_table_and_column_for_station_type(station_type)
    schema = os.getenv("HUBEAU_SCHEMA", "hubeau")

    # Extraire l'année de la partition
    year = datetime.strptime(partition_date, "%Y-%m-%d").year
    year_start = f"{year}-01-01"
    year_end = f"{year}-12-31"

    logger.debug(f"🔍 Filtrage stations actives pour année {year}")

    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                # Vérifier quelles colonnes temporelles existent
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s
                    AND table_name = %s
                    AND column_name IN ('date_debut_mesure', 'date_fin_mesure', 'date_debut', 'date_fin')
                """, (schema, table))

                temporal_columns = {row[0] for row in cur.fetchall()}

                if not temporal_columns:
                    logger.info(f"ℹ️ Pas de colonnes temporelles dans {table}, retour toutes stations")
                    return stations

                # Construire query de filtrage selon colonnes disponibles
                query = f"""
                    SELECT DISTINCT {code_column}
                    FROM {schema}.{table}
                    WHERE {code_column} = ANY(%s)
                """

                params = [stations]

                if 'date_debut_mesure' in temporal_columns and 'date_fin_mesure' in temporal_columns:
                    query += """
                        AND (date_debut_mesure <= %s::date OR date_debut_mesure IS NULL)
                        AND (date_fin_mesure >= %s::date OR date_fin_mesure IS NULL)
                    """
                    params.extend([year_end, year_start])
                    logger.debug("   Filtrage avec date_debut_mesure/date_fin_mesure")

                elif 'date_debut' in temporal_columns and 'date_fin' in temporal_columns:
                    query += """
                        AND (date_debut <= %s::date OR date_debut IS NULL)
                        AND (date_fin >= %s::date OR date_fin IS NULL)
                    """
                    params.extend([year_end, year_start])
                    logger.debug("   Filtrage avec date_debut/date_fin")

                query += f" ORDER BY {code_column}"

                cur.execute(query, params)
                filtered = [row[0] for row in cur.fetchall()]

                logger.info(f"✅ {len(filtered)}/{len(stations)} stations actives pour année {year}")

                return filtered

    except psycopg.Error as e:
        logger.error(f"❌ Erreur PostgreSQL lors du filtrage: {e}")
        logger.warning(f"   Retour de toutes les stations ({len(stations)}) sans filtrage")
        return stations  # Retourner toutes en cas d'erreur
