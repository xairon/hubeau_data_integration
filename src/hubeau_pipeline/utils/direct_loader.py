"""
Direct PostgreSQL loader for Hub'Eau data.
Replaces DLT with simple psycopg2 for full control and debugging.
"""
import os
import logging
from typing import Any, Dict, Iterator, List, Optional

import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


def get_postgres_connection():
    """Create PostgreSQL connection from environment variables."""
    return psycopg2.connect(
        host=os.getenv('PG_HOST', 'postgres'),
        port=os.getenv('PG_PORT', '5432'),
        database=os.getenv('PG_DB', 'postgres'),
        user=os.getenv('PG_USER', 'postgres'),
        password=os.getenv('PG_PASSWORD')
    )


def ensure_table_exists(conn, table_name: str, sample_record: Dict[str, Any]):
    """
    Create table if it doesn't exist, based on sample record structure.
    All columns are TEXT for simplicity (bronze layer = raw data).
    """
    schema = "staging"
    full_table_name = f"{schema}.{table_name}"
    
    # Get column names from sample record
    columns = list(sample_record.keys())
    
    # Build CREATE TABLE statement (all columns as TEXT)
    columns_sql = ", ".join([f'"{col}" TEXT' for col in columns])
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
        {columns_sql},
        _loaded_at TIMESTAMP DEFAULT NOW(),
        _partition_year TEXT
    )
    """
    
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()
    
    logger.info(f"✅ Table {full_table_name} ready with {len(columns)} columns")
    return columns


def load_records_to_postgres(
    data_generator: Iterator[List[Dict]],
    table_name: str,
    partition_year: str,
    dagster_context=None,
    batch_commit_size: int = 10
) -> Dict[str, Any]:
    """
    Load records from generator directly to PostgreSQL.
    
    Args:
        data_generator: Generator yielding List[Dict] batches
        table_name: Target table name (without schema)
        partition_year: Year partition for tracking
        dagster_context: Optional Dagster context for logging
        batch_commit_size: Commit every N batches
    
    Returns:
        Dict with loading statistics
    """
    def log_info(msg: str):
        logger.info(msg)
        if dagster_context:
            dagster_context.log.info(msg)
    
    def log_error(msg: str):
        logger.error(msg)
        if dagster_context:
            dagster_context.log.error(msg)
    
    schema = "staging"
    full_table_name = f"{schema}.{table_name}"
    
    conn = get_postgres_connection()
    total_rows = 0
    batch_count = 0
    columns = None
    
    try:
        for batch in data_generator:
            if not batch:
                continue
            
            # First batch: ensure table exists and get columns
            if columns is None:
                columns = ensure_table_exists(conn, table_name, batch[0])
                log_info(f"📊 Table columns: {len(columns)}")
            
            # Prepare values for insertion
            values = []
            for record in batch:
                row = [str(record.get(col, '')) if record.get(col) is not None else None 
                       for col in columns]
                row.append(None)  # _loaded_at (default)
                row.append(partition_year)  # _partition_year
                values.append(tuple(row))
            
            # Insert batch
            columns_with_meta = columns + ['_loaded_at', '_partition_year']
            insert_sql = f"""
                INSERT INTO {full_table_name} ({', '.join([f'"{c}"' for c in columns_with_meta])})
                VALUES %s
            """
            
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, values, template=None, page_size=1000)
            
            total_rows += len(batch)
            batch_count += 1
            
            # Commit periodically
            if batch_count % batch_commit_size == 0:
                conn.commit()
                log_info(f"✅ Committed {total_rows:,} rows ({batch_count} batches)")
        
        # Final commit
        conn.commit()
        log_info(f"🎉 COMPLETE: {total_rows:,} rows loaded to {full_table_name}")
        
        return {
            "rows_loaded": total_rows,
            "batch_count": batch_count,
            "table_name": full_table_name,
            "partition_year": partition_year,
            "status": "success"
        }
        
    except Exception as e:
        conn.rollback()
        log_error(f"❌ Error loading data: {e}")
        raise
    finally:
        conn.close()


def delete_partition(table_name: str, partition_year: str, dagster_context=None) -> int:
    """
    Delete existing data for a specific partition before reloading.
    Returns number of rows deleted.
    """
    def log_info(msg: str):
        logger.info(msg)
        if dagster_context:
            dagster_context.log.info(msg)
    
    schema = "staging"
    full_table_name = f"{schema}.{table_name}"
    
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                )
            """, (schema, table_name))
            
            if not cur.fetchone()[0]:
                log_info(f"Table {full_table_name} doesn't exist yet, nothing to delete")
                return 0
            
            # Delete partition data
            cur.execute(f"""
                DELETE FROM {full_table_name} 
                WHERE _partition_year = %s
            """, (partition_year,))
            
            deleted = cur.rowcount
            conn.commit()
            
            if deleted > 0:
                log_info(f"🗑️ Deleted {deleted:,} rows for partition {partition_year}")
            
            return deleted
            
    finally:
        conn.close()
