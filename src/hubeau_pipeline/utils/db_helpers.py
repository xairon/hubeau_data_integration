"""
Database helper functions for Hub'Eau pipeline
Bronze layer utilities for managing raw tables
"""
import os
import psycopg2
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def get_connection():
    """
    Get PostgreSQL connection using environment variables

    Returns:
        psycopg2 connection object
    """
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"),
        port=os.getenv("PG_PORT", "5432"),
        database=os.getenv("PG_DB", "hubeau"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD")
    )


def delete_year_data(
    table_name: str,
    year: str,
    date_column: str,
    schema: str = "hubeau"
) -> int:
    """
    Delete all data for a specific year from a table (or TRUNCATE for "full" mode)
    Used before re-loading year partitions to ensure idempotency

    Args:
        table_name: Name of the table (e.g., "temperature_chroniques_raw")
        year: Year to delete (e.g., "2024") or "full" to truncate entire table
        date_column: Name of the date column (e.g., "date_mesure_temp")
        schema: Schema name (default: "hubeau")

    Returns:
        Number of rows deleted

    Example:
        >>> deleted = delete_year_data("temperature_chroniques_raw", "2024", "date_mesure_temp")
        >>> print(f"Deleted {deleted} rows")
        >>> deleted = delete_year_data("temperature_chroniques_raw", "full", "date_mesure_temp")
        >>> print(f"Truncated table, deleted {deleted} rows")
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Check if table exists first
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
        """, (schema, table_name))

        if not cursor.fetchone()[0]:
            logger.info(f"Table {schema}.{table_name} does not exist yet, skipping delete")
            return 0

        if year == "full":
            # FULL MODE: TRUNCATE entire table (load ALL historical data)
            # First count rows
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
            deleted_count = cursor.fetchone()[0]

            # Then truncate
            truncate_sql = f"TRUNCATE TABLE {schema}.{table_name}"
            cursor.execute(truncate_sql)
            conn.commit()

            logger.info(f"TRUNCATED {schema}.{table_name} (deleted {deleted_count} rows) for FULL load")
            return deleted_count
        else:
            # YEAR MODE: Delete only specific year
            delete_sql = f"""
                DELETE FROM {schema}.{table_name}
                WHERE EXTRACT(YEAR FROM {date_column}) = %s
            """
            cursor.execute(delete_sql, (year,))

            deleted_count = cursor.rowcount
            conn.commit()

            logger.info(f"Deleted {deleted_count} rows from {schema}.{table_name} for year {year}")
            return deleted_count

    except Exception as e:
        conn.rollback()
        logger.error(f"Error deleting year data: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def get_max_date(
    table_name: str,
    date_column: str,
    schema: str = "hubeau"
) -> Optional[str]:
    """
    Get the maximum date from a table for incremental loading

    Args:
        table_name: Name of the table
        date_column: Name of the date column
        schema: Schema name (default: "hubeau")

    Returns:
        Maximum date as string (YYYY-MM-DD) or None if table is empty

    Example:
        >>> max_date = get_max_date("temperature_chroniques_raw", "date_mesure_temp")
        >>> print(f"Last date: {max_date}")
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
        """, (schema, table_name))

        if not cursor.fetchone()[0]:
            logger.info(f"Table {schema}.{table_name} does not exist yet")
            return None

        # Get max date
        max_sql = f"""
            SELECT MAX({date_column})::date
            FROM {schema}.{table_name}
        """
        cursor.execute(max_sql)
        result = cursor.fetchone()[0]

        if result:
            logger.info(f"Max date in {schema}.{table_name}: {result}")
            return str(result)
        else:
            logger.info(f"Table {schema}.{table_name} is empty")
            return None

    except Exception as e:
        logger.error(f"Error getting max date: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def table_exists(table_name: str, schema: str = "hubeau") -> bool:
    """
    Check if a table exists in the database

    Args:
        table_name: Name of the table
        schema: Schema name (default: "hubeau")

    Returns:
        True if table exists, False otherwise
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
        """, (schema, table_name))

        return cursor.fetchone()[0]

    finally:
        cursor.close()
        conn.close()
