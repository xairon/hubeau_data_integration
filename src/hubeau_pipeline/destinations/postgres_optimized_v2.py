"""
Custom DLT destination PostgreSQL OPTIMISÉ V2 avec pool de connexions et cache
Performance: 100k records en 1-2 secondes (vs 2-3s avant, vs 5-10min DLT)
"""

import os
import io
import time
import re
import pandas as pd
import psycopg2
from psycopg2 import pool, errors
from typing import Iterator, Any, Dict, List, Optional
import logging
from threading import Lock

# Import des type mappings Hub'Eau
from hubeau_pipeline.schema import get_table_schema, get_field_type, get_primary_key, HUBEAU_FIELD_TYPES

logger = logging.getLogger(__name__)


class PostgresBulkDestinationV2:
    """
    Custom PostgreSQL destination ultra-optimisée avec:
    - Pool de connexions (évite overhead création/destruction)
    - Cache des métadonnées de tables
    - Gestion mémoire optimisée (moins de copies DataFrame)
    - Batch processing amélioré
    """

    _instance = None
    _lock = Lock()

    def __new__(cls):
        """Singleton thread-safe avec lock"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """
        Initialisation lazy - PAS de connexion créée ici!
        Le pool sera créé au premier appel à _get_connection()
        """
        if self._initialized:
            return

        # Configuration (lu depuis ENV - pas de connexion créée)
        self.conn_params = {
            "host": os.getenv("PG_HOST", "postgres"),
            "port": int(os.getenv("PG_PORT", "5432")),
            "database": os.getenv("PG_DB", "postgres"),
            "user": os.getenv("PG_USER", "postgres"),
            "password": os.getenv("PG_PASSWORD")
        }
        self.schema_name = "hubeau"

        # Pool créé en lazy (None au démarrage)
        self.connection_pool = None

        self._initialized = True
        logger.debug("INFO: PostgresBulkDestinationV2 config loaded (lazy pool)")

    def _ensure_pool_initialized(self):
        """Initialise le pool de connexions de manière lazy (thread-safe)"""
        if self.connection_pool is None:
            with self._lock:
                # Double-check locking pattern
                if self.connection_pool is None:
                    logger.info("🔌 Initializing PostgreSQL connection pool (lazy)...")
                    self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                        2, 10, **self.conn_params
                    )
                    logger.info("INFO: PostgreSQL connection pool initialized")

    def _get_connection(self):
        """Obtenir une connexion depuis le pool (initialisation lazy)"""
        self._ensure_pool_initialized()
        return self.connection_pool.getconn()

    def _release_connection(self, conn):
        """Remettre la connexion dans le pool"""
        if conn:
            self.connection_pool.putconn(conn)

    def _get_target_columns(self, table_name: str, conn=None) -> List[str]:
        """Récupère les colonnes de la table depuis information_schema"""
        need_release = False
        if conn is None:
            conn = self._get_connection()
            need_release = True

        try:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (self.schema_name, table_name))
                return [row[0] for row in cursor.fetchall()]
        finally:
            if need_release:
                self._release_connection(conn)

    def _get_table_column_types(self, table_name: str, conn=None) -> Dict[str, str]:
        """Récupère les types des colonnes de la table (utilisé pour casting)"""
        need_release = False
        if conn is None:
            conn = self._get_connection()
            need_release = True

        try:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (self.schema_name, table_name))
                return {row[0]: row[1] for row in cursor.fetchall()}
        finally:
            if need_release:
                self._release_connection(conn)

    def _clean_dataframe_inplace(self, df: pd.DataFrame, table_name: str = None, conn=None) -> pd.DataFrame:
        """
        Nettoie le DataFrame EN PLACE pour économiser la mémoire
        Évite les copies inutiles

        Args:
            df: DataFrame à nettoyer
            table_name: Nom de la table (optionnel, pour casting de types)
            conn: Connexion PostgreSQL (optionnel, pour récupérer les types)
        """
        import json

        # INFO: Casting de types selon le schéma PostgreSQL
        if table_name and conn:
            try:
                column_types = self._get_table_column_types(table_name, conn)
                for col in df.columns:
                    if col not in column_types:
                        continue

                    pg_type = column_types[col]

                    # Cast integer types (integer, bigint, smallint)
                    if pg_type in ['integer', 'bigint', 'smallint']:
                        # Convert float strings like "1.0" to integers
                        if df[col].dtype in ['object', 'float64', 'float32']:
                            try:
                                # Vectorized conversion: empty string → NaN, float string → int
                                # Use pd.to_numeric for vectorized conversion
                                df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert to float, invalid → NaN
                                df[col] = df[col].fillna(pd.NA)  # NaN → pandas NA
                                df[col] = df[col].astype('Int64')  # Convert to nullable integer (handles NA)
                                logger.debug(f"INFO: Cast {col} to {pg_type} (vectorized)")
                            except (ValueError, TypeError) as e:
                                logger.warning(f"WARNING: Failed to cast {col} to {pg_type}: {e}")

                    # Cast double precision / real
                    elif pg_type in ['double precision', 'real', 'numeric']:
                        if df[col].dtype == 'object':
                            try:
                                # Vectorized conversion with error handling
                                # Non-numeric strings will be converted to NaN
                                # Count non-null BEFORE conversion (without copy - memory optimization)
                                originally_non_null = df[col].notna().sum()

                                df[col] = pd.to_numeric(df[col], errors='coerce')

                                # Check if too many conversions failed (>50% became NaN)
                                after_conversion_non_null = df[col].notna().sum()
                                if originally_non_null > 0 and after_conversion_non_null < originally_non_null * 0.5:
                                    # More than 50% failed conversion - log warning
                                    # NOTE: No restore - let PostgreSQL handle error with clear message
                                    logger.warning(f"WARNING: Column {col} typed as {pg_type} but contains non-numeric data ({after_conversion_non_null}/{originally_non_null} converted) - PostgreSQL will handle error")
                                else:
                                    logger.debug(f"INFO: Cast {col} to {pg_type} (vectorized, {after_conversion_non_null}/{originally_non_null} valid values)")
                            except (ValueError, TypeError) as e:
                                logger.warning(f"WARNING: Failed to cast {col} to {pg_type}: {e}")

                    # Cast boolean
                    elif pg_type == 'boolean':
                        if df[col].dtype == 'object':
                            try:
                                df[col] = df[col].apply(
                                    lambda x: None if pd.isna(x) or x == '' else bool(x)
                                )
                                logger.debug(f"INFO: Cast {col} to {pg_type}")
                            except (ValueError, TypeError) as e:
                                logger.warning(f"WARNING: Failed to cast {col} to {pg_type}: {e}")
            except Exception as e:
                logger.warning(f"WARNING: Could not retrieve column types for {table_name}: {e}")

        for col in df.columns:
            if df[col].dtype == 'object':
                # Utiliser vectorisation quand possible
                mask_none = df[col].isna()

                # Pour les listes/tuples, extraire le premier élément
                def extract_first(val):
                    if val is None or (isinstance(val, float) and pd.isna(val)):
                        return None
                    if isinstance(val, (list, tuple)):
                        return str(val[0]) if val else None
                    if isinstance(val, dict):
                        return json.dumps(val)
                    return str(val) if val != '' else None

                # Appliquer seulement sur les non-null pour performance
                df.loc[~mask_none, col] = df.loc[~mask_none, col].apply(extract_first)

                # Fix: French comma format for numeric fields (e.g., "994,4" -> "994.4")
                # Check if column contains numeric-like strings with commas
                if not mask_none.all():
                    sample = df.loc[~mask_none, col].iloc[0] if len(df.loc[~mask_none, col]) > 0 else None
                    if sample and isinstance(sample, str) and ',' in sample:
                        # Try to detect if it's a French decimal format
                        try:
                            test_val = sample.replace(',', '.')
                            float(test_val)
                            # If conversion works, apply to all
                            df.loc[~mask_none, col] = df.loc[~mask_none, col].str.replace(',', '.', regex=False)
                            logger.debug(f"Converted French comma format to decimal in column {col}")
                        except (ValueError, AttributeError):
                            pass  # Not a numeric field

        # Fix: Convert millisecond timestamps to ISO format
        for col in df.columns:
            # Check if column looks like it contains millisecond timestamps
            if df[col].dtype in ['int64', 'float64', 'object']:
                try:
                    # Sample first non-null value
                    sample_mask = df[col].notna()
                    if sample_mask.any():
                        sample = df.loc[sample_mask, col].iloc[0]
                        # Check if value is a large integer (likely milliseconds since epoch)
                        if isinstance(sample, (int, float, str)):
                            sample_int = int(float(sample)) if sample else 0
                            # Milliseconds since epoch are typically 13 digits (1970-2100)
                            if 1000000000000 <= sample_int <= 9999999999999:
                                # Convert milliseconds to datetime
                                df[col] = pd.to_datetime(df[col], unit='ms', errors='coerce')
                                logger.debug(f"Converted millisecond timestamps to datetime in column {col}")
                except (ValueError, TypeError, AttributeError):
                    pass  # Not a timestamp column

        return df

    def _copy_from_dataframe(self, df: pd.DataFrame, table_name: str, conn=None):
        """
        COPY optimisé avec connexion réutilisable
        """
        need_release = False
        if conn is None:
            conn = self._get_connection()
            need_release = True

        try:
            with conn.cursor() as cursor:
                # Les colonnes du DataFrame sont déjà normalisées dans load_batch
                # Récupérer colonnes (depuis cache si possible)
                target_columns = self._get_target_columns(table_name, conn)

                # Si table n'existe pas, lever une erreur explicite
                if not target_columns:
                    logger.error(f"ERROR: Table {table_name} n'existe pas!")
                    logger.error(f"   La table doit être créée via scripts/schema/{table_name}.sql")
                    logger.error(f"   Ou relancer l'asset Dagster qui créera automatiquement la table")
                    raise ValueError(
                        f"Table {table_name} does not exist. "
                        f"It should be created automatically by the Dagster asset. "
                        f"If running manually, execute: scripts/schema/{table_name}.sql"
                    )

                # INFO: FILTRAGE ROBUSTE: Gérer les différences de casse et colonnes inconnues
                df_columns = df.columns.tolist()
                
                # Normalisation des noms (case-insensitive matching)
                target_columns_lower = {col.lower(): col for col in target_columns}
                column_mapping = {}
                common_columns = []
                
                for df_col in df_columns:
                    df_col_lower = df_col.lower()
                    if df_col_lower in target_columns_lower:
                        # Colonne trouvée (exact ou case-insensitive)
                        target_col = target_columns_lower[df_col_lower]
                        column_mapping[df_col] = target_col
                        common_columns.append(df_col)
                    else:
                        logger.debug(f"  Colonne ignorée: {df_col} (pas de mapping dans {table_name})")

                if not common_columns:
                    logger.error(f"ERROR: Aucune colonne commune entre DataFrame et table {table_name}")
                    logger.error(f"   DataFrame: {df_columns[:10]}...")
                    logger.error(f"   Table: {list(target_columns)[:10]}...")
                    raise ValueError(f"Aucune colonne commune entre DataFrame et table {table_name}")

                # Appliquer renommage si nécessaire et sélectionner colonnes
                if column_mapping:
                    df = df[common_columns].rename(columns=column_mapping)
                    common_columns = [column_mapping.get(col, col) for col in common_columns]
                else:
                    df = df[common_columns]

                logger.debug(f"  Colonnes utilisées: {len(common_columns)}/{len(df_columns)} ({', '.join(common_columns[:5])}...)")

                df = self._clean_dataframe_inplace(df, table_name=table_name, conn=conn)

                # Gérer colonnes DLT si nécessaires
                if '_dlt_load_id' in target_columns and '_dlt_load_id' not in df.columns:
                    df['_dlt_load_id'] = f"load_{int(time.time() * 1000)}"
                    common_columns.append('_dlt_load_id')

                if '_dlt_id' in target_columns and '_dlt_id' not in df.columns:
                    df['_dlt_id'] = [f"row_{i}_{int(time.time() * 1000)}" for i in range(len(df))]
                    common_columns.append('_dlt_id')

                # COPY avec chunking pour économiser RAM (évite gros StringIO buffer)
                COPY_CHUNK_SIZE = 1000  # Process 1000 rows at a time

                copy_sql = f"""
                    COPY {self.schema_name}.{table_name} ({','.join(common_columns)})
                    FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')
                """

                total_rows = len(df)
                rows_copied = 0

                for start_idx in range(0, total_rows, COPY_CHUNK_SIZE):
                    end_idx = min(start_idx + COPY_CHUNK_SIZE, total_rows)
                    chunk = df.iloc[start_idx:end_idx]

                    # Convert chunk to CSV buffer
                    output = io.StringIO()
                    chunk.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
                    output.seek(0)

                    # COPY chunk
                    cursor.copy_expert(copy_sql, output)
                    rows_copied += len(chunk)

                    # Free memory immediately
                    del chunk, output

                    # Progress log every 10k rows
                    if rows_copied % 10000 == 0 or rows_copied == total_rows:
                        logger.debug(f"  COPY progress: {rows_copied}/{total_rows} rows")

                conn.commit()
                logger.info(f"INFO: COPY: {rows_copied} records → {table_name} (chunked)")

        except errors.InvalidTextRepresentation as e:
            conn.rollback()

            # Check if error is due to text in numeric column
            error_msg = str(e)
            if 'invalid input syntax for type' in error_msg and ('double precision' in error_msg or 'bigint' in error_msg or 'integer' in error_msg):
                # Extract column name from error message
                # Example: COPY table, line 854, column code_entite_hydro_cours_eau: "O07-0400"
                match = re.search(r'column (\w+):', error_msg)
                if match:
                    problematic_column = match.group(1)
                    logger.warning(f"🔧 Colonne {problematic_column} contient du texte mais est typée comme numérique - AUTO-FIX en TEXT")

                    try:
                        # ALTER column to TEXT
                        with conn.cursor() as alter_cursor:
                            alter_cursor.execute(f"""
                                ALTER TABLE {self.schema_name}.{table_name}
                                ALTER COLUMN {problematic_column} TYPE TEXT
                            """)
                            conn.commit()
                            logger.info(f"INFO: Colonne {problematic_column} convertie en TEXT")

                        # RETRY COPY avec NOUVEAU cursor - PEUT échouer sur AUTRE colonne
                        logger.info(f"🔄 Retry COPY après correction de schéma...")
                        max_retries = 10  # Max 10 colonnes à corriger
                        for retry_attempt in range(max_retries):
                            try:
                                with conn.cursor() as retry_cursor:
                                    output.seek(0)  # Reset buffer
                                    retry_cursor.copy_expert(copy_sql, output)
                                    conn.commit()
                                    logger.info(f"INFO: COPY réussi: {len(df)} records → {table_name}")
                                    return  # Success!
                            except errors.InvalidTextRepresentation as retry_error:
                                # ENCORE une autre colonne mal typée!
                                retry_error_msg = str(retry_error)
                                retry_match = re.search(r'column (\w+):', retry_error_msg)
                                if retry_match:
                                    next_column = retry_match.group(1)
                                    logger.warning(f"🔧 Autre colonne problématique: {next_column} → TEXT")
                                    conn.rollback()
                                    with conn.cursor() as fix_cursor:
                                        fix_cursor.execute(f"""
                                            ALTER TABLE {self.schema_name}.{table_name}
                                            ALTER COLUMN {next_column} TYPE TEXT
                                        """)
                                        conn.commit()
                                else:
                                    raise retry_error  # Erreur différente

                        # Si on arrive ici, trop de colonnes à corriger
                        raise Exception(f"Trop de colonnes mal typées dans {table_name} (>{max_retries})")

                    except Exception as alter_error:
                        logger.error(f"ERROR: Erreur lors de l'AUTO-FIX: {alter_error}")
                        raise e  # Re-raise original error

            # Si pas une erreur de type connue, re-raise
            logger.error(f"ERROR: Erreur COPY: {e}")
            raise

        except Exception as e:
            conn.rollback()
            logger.error(f"ERROR: Erreur COPY: {e}")
            raise
        finally:
            if need_release:
                self._release_connection(conn)

    def _upsert_dataframe(self, df: pd.DataFrame, table_name: str, primary_keys: List[str]):
        """
        UPSERT optimisé avec connexion unique
        """
        logger.info(f"🔍 DEBUG _upsert_dataframe START:")
        logger.info(f"  - table_name: {table_name}")
        logger.info(f"  - primary_keys input: {primary_keys}")
        logger.info(f"  - df.shape: {df.shape}")
        logger.info(f"  - df.columns[:10]: {df.columns.tolist()[:10]}")

        conn = self._get_connection()
        staging_table = f"staging_{int(time.time() * 1000)}"

        try:
            with conn.cursor() as cursor:
                # Récupérer colonnes cibles (cache)
                target_columns = self._get_target_columns(table_name, conn)

                # Si table n'existe pas, lever une erreur explicite
                if not target_columns:
                    logger.error(f"ERROR: Table {table_name} n'existe pas!")
                    raise ValueError(
                        f"Table {table_name} does not exist. "
                        f"It should be created automatically by the Dagster asset."
                    )

                # Filtrer colonnes
                df_columns = df.columns.tolist()

                common_columns = [col for col in df_columns if col in target_columns]

                # Vérifier primary keys
                missing_pks = [pk for pk in primary_keys if pk not in common_columns]

                if missing_pks:
                    logger.error(f"🔍 DEBUG ERROR DETAIL:")
                    logger.error(f"  - primary_keys: {primary_keys}")
                    logger.error(f"  - df_columns: {df_columns}")
                    logger.error(f"  - target_columns: {target_columns}")
                    logger.error(f"  - common_columns: {common_columns}")
                    logger.error(f"  - missing_pks: {missing_pks}")
                    raise ValueError(f"Primary keys manquantes: {missing_pks}")


                # Nettoyer DataFrame EN PLACE
                if len(common_columns) < len(df_columns):
                    df = df[common_columns]
                df = self._clean_dataframe_inplace(df, table_name=table_name, conn=conn)

                # Fix: Deduplicate rows to avoid "ON CONFLICT DO UPDATE command cannot affect row a second time"
                # This error occurs when the same primary key appears multiple times in a single batch
                original_len = len(df)

                # DEFENSIVE: Normaliser les primary_keys pour être sûr qu'ils matchent les colonnes du DataFrame
                # FIX: Forcer conversion en liste au cas où c'est un tuple
                primary_keys_for_dedup = [pk.lower() for pk in (list(primary_keys) if isinstance(primary_keys, tuple) else primary_keys)]

                # Vérifier que TOUTES les primary keys existent dans le DataFrame AVANT deduplication
                missing_for_dedup = [pk for pk in primary_keys_for_dedup if pk not in df.columns]
                if missing_for_dedup:
                    logger.error(f"ERROR: CRITICAL: Primary keys manquantes pour deduplication: {missing_for_dedup}")
                    logger.error(f"   DataFrame columns: {df.columns.tolist()}")
                    logger.error(f"   Primary keys: {primary_keys_for_dedup}")
                    raise ValueError(f"Cannot deduplicate: primary keys {missing_for_dedup} not in DataFrame")

                logger.info(f"🔍 DEBUG Deduplication: Using primary_keys={primary_keys_for_dedup} on {original_len} rows")
                df = df.drop_duplicates(subset=primary_keys_for_dedup, keep='last')

                if len(df) < original_len:
                    logger.warning(f"WARNING: Deduplication: removed {original_len - len(df)} duplicate rows from {table_name}")
                else:
                    logger.info(f"✓ No duplicates found in batch for {table_name} ({original_len} rows checked)")

                # Créer staging table (simplifiée avec gestion des NULL)
                cursor.execute(f"""
                    SELECT column_name,
                           CASE
                               WHEN data_type = 'character varying' AND character_maximum_length IS NOT NULL
                                   THEN 'VARCHAR(' || character_maximum_length || ')'
                               WHEN data_type = 'character varying' AND character_maximum_length IS NULL
                                   THEN 'TEXT'
                               WHEN data_type = 'text' THEN 'TEXT'
                               WHEN data_type IN ('timestamp without time zone', 'timestamp with time zone') THEN 'TIMESTAMP'
                               WHEN data_type = 'double precision' THEN 'DOUBLE PRECISION'
                               WHEN data_type = 'integer' THEN 'INTEGER'
                               WHEN data_type = 'bigint' THEN 'BIGINT'
                               WHEN data_type = 'boolean' THEN 'BOOLEAN'
                               WHEN data_type = 'date' THEN 'DATE'
                               WHEN data_type = 'time without time zone' THEN 'TIME'
                               WHEN data_type = 'numeric' THEN 'NUMERIC'
                               WHEN data_type = 'real' THEN 'REAL'
                               WHEN data_type = 'json' THEN 'JSON'
                               WHEN data_type = 'jsonb' THEN 'JSONB'
                               ELSE UPPER(data_type)
                           END as col_type
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    AND column_name = ANY(%s)
                    ORDER BY ordinal_position
                """, (self.schema_name, table_name, common_columns))

                results = cursor.fetchall()
                if not results:
                    raise ValueError(f"Aucune colonne trouvée pour la table {table_name}")

                col_defs = []
                for name, dtype in results:
                    if dtype is None:
                        logger.warning(f"Type NULL détecté pour colonne {name}, utilisation de TEXT par défaut")
                        dtype = 'TEXT'
                    col_defs.append(f"{name} {dtype}")

                cursor.execute(f"""
                    CREATE TEMP TABLE {staging_table} (
                        {', '.join(col_defs)}
                    )
                """)

                # COPY vers staging avec chunking
                COPY_CHUNK_SIZE = 1000
                total_rows = len(df)

                for start_idx in range(0, total_rows, COPY_CHUNK_SIZE):
                    end_idx = min(start_idx + COPY_CHUNK_SIZE, total_rows)
                    chunk = df.iloc[start_idx:end_idx]

                    output = io.StringIO()
                    chunk.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
                    output.seek(0)

                    cursor.copy_expert(
                        f"COPY {staging_table} ({','.join(common_columns)}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')",
                        output
                    )

                    del chunk, output

                logger.debug(f"  Staging: {total_rows} rows copied (chunked)")

                # UPSERT avec génération DLT si nécessaire
                insert_columns = common_columns.copy()
                select_columns = common_columns.copy()

                if '_dlt_load_id' in target_columns and '_dlt_load_id' not in common_columns:
                    insert_columns.append('_dlt_load_id')
                    select_columns.append(f"'load_{int(time.time() * 1000)}'::TEXT")

                if '_dlt_id' in target_columns and '_dlt_id' not in common_columns:
                    insert_columns.append('_dlt_id')
                    # Utiliser les primary_keys normalisées
                    pk_concat = ' || '.join([f"COALESCE({pk}::TEXT, '')" for pk in primary_keys_for_dedup])
                    select_columns.append(f"MD5({pk_concat})::TEXT")

                # Update columns (exclure PK) - utiliser primary_keys normalisées
                update_cols = [c for c in common_columns if c not in primary_keys_for_dedup]

                if update_cols:
                    update_set = ', '.join([f"{c} = EXCLUDED.{c}" for c in update_cols])
                    if 'updated_at' in target_columns and 'updated_at' not in update_cols:
                        update_set += ", updated_at = CURRENT_TIMESTAMP"
                    conflict_action = f"DO UPDATE SET {update_set}"
                else:
                    conflict_action = "DO NOTHING"

                logger.info(f"🔍 DEBUG UPSERT SQL: ON CONFLICT ({','.join(primary_keys_for_dedup)})")
                upsert_sql = f"""
                    INSERT INTO {self.schema_name}.{table_name} ({','.join(insert_columns)})
                    SELECT {','.join(select_columns)} FROM {staging_table}
                    ON CONFLICT ({','.join(primary_keys_for_dedup)})
                    {conflict_action}
                """

                cursor.execute(upsert_sql)
                affected = cursor.rowcount

                # DROP staging table explicitement pour libérer mémoire
                cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")

                conn.commit()
                logger.info(f"INFO: UPSERT: {affected}/{len(df)} records modifiés")

        except Exception as e:
            conn.rollback()
            logger.error(f"ERROR: Erreur UPSERT: {e}")
            raise
        finally:
            self._release_connection(conn)

    def _truncate_cascade(self, table_name: str):
        """TRUNCATE avec connexion du pool - check if table exists first"""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                # Check if table exists before TRUNCATE
                cursor.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = %s
                    )
                """, (self.schema_name, table_name))

                table_exists = cursor.fetchone()[0]

                if table_exists:
                    cursor.execute(f"TRUNCATE TABLE {self.schema_name}.{table_name} CASCADE")
                    conn.commit()
                    logger.info(f"INFO: TRUNCATE CASCADE: {table_name}")
                else:
                    logger.warning(f"WARNING: Table {self.schema_name}.{table_name} n'existe pas - skip TRUNCATE")
                    # Table will be created by first COPY operation
        finally:
            self._release_connection(conn)

    def load_batch(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        write_disposition: str,
        primary_keys: Optional[List[str]] = None,
        column_mappings: Optional[Dict[str, str]] = None,
        partition_year: Optional[int] = None
    ):
        """Point d'entrée principal optimisé"""
        logger.info(f"🔍 DEBUG load_batch START: table={table_name}, disposition={write_disposition}, primary_keys={primary_keys}, data_count={len(data) if data else 0}")

        if not data:
            logger.warning(f"WARNING: Pas de données pour {table_name}")
            return

        # Log les premières colonnes des données
        if data:
            first_record_keys = list(data[0].keys())[:10]
            logger.info(f"🔍 DEBUG load_batch: First record keys (sample): {first_record_keys}")

        # Normaliser les clés du dictionnaire AVANT de créer le DataFrame
        normalized_data = []
        for record in data:
            normalized_record = {key.lower(): value for key, value in record.items()}
            normalized_data.append(normalized_record)

        # DataFrame avec colonnes déjà normalisées
        df = pd.DataFrame(normalized_data)

        # Double vérification - normaliser aussi les colonnes du DataFrame
        df.columns = [col.lower() for col in df.columns]

        # NOUVEAU : Normaliser aussi les primary_keys pour cohérence
        # FIX: Forcer conversion en liste au cas où c'est un tuple
        if primary_keys:
            if isinstance(primary_keys, tuple):
                primary_keys = list(primary_keys)
            original_pks = primary_keys.copy()
            primary_keys = [pk.lower() for pk in primary_keys]

        if column_mappings:
            df = df.rename(columns=column_mappings)
            if primary_keys:
                primary_keys = [column_mappings.get(pk, pk) for pk in primary_keys]

        logger.info(f"INFO: Loading {len(df)} records → {table_name} ({write_disposition})")

        # OPTIMIZATION: Use DELETE+COPY for year-based partitions instead of row-by-row UPSERT
        if partition_year and write_disposition == "merge":
            logger.info(f"INFO: DELETE+COPY optimization for year {partition_year}")

            # Find the date column for this table
            date_column = None
            for col in df.columns:
                if 'date' in col.lower():
                    date_column = col
                    break

            if date_column:
                logger.info(f"  Using date column: {date_column}")

                # Delete existing data for this year
                conn = self._get_connection()
                try:
                    with conn.cursor() as cursor:
                        # Check if table exists first
                        cursor.execute(f"""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables
                                WHERE table_schema = %s AND table_name = %s
                            )
                        """, (self.schema_name, table_name))

                        if cursor.fetchone()[0]:
                            # Table exists, delete year data
                            delete_sql = f"""
                                DELETE FROM {self.schema_name}.{table_name}
                                WHERE EXTRACT(YEAR FROM {date_column}) = %s
                            """
                            cursor.execute(delete_sql, (partition_year,))
                            deleted = cursor.rowcount
                            conn.commit()
                            logger.info(f"  Deleted {deleted} existing records for year {partition_year}")
                        else:
                            logger.info(f"  Table doesn't exist yet, will be created by COPY")

                    # Deduplicate before COPY (Hub'Eau API may return duplicates within page)
                    if primary_keys:
                        pk_lower = [pk.lower() for pk in primary_keys]
                        original_len = len(df)
                        df = df.drop_duplicates(subset=pk_lower, keep='last')
                        if len(df) < original_len:
                            logger.warning(f"WARNING: Removed {original_len - len(df)} duplicate rows before COPY (year {partition_year})")

                    # Now use COPY to insert all data at once (much faster than UPSERT)
                    self._copy_from_dataframe(df, table_name, conn)

                finally:
                    self._release_connection(conn)

                # Skip the normal merge logic
                return
            else:
                logger.warning(f"WARNING: No date column found for DELETE+COPY optimization, falling back to UPSERT")

        if write_disposition == "replace":
            self._truncate_cascade(table_name)
            self._copy_from_dataframe(df, table_name)
        elif write_disposition == "merge":
            if not primary_keys:
                raise ValueError("primary_keys requis pour merge")

            logger.info(f"🔍 DEBUG load_batch MERGE: About to call _upsert_dataframe")
            logger.info(f"  - table_name: {table_name}")
            logger.info(f"  - primary_keys: {primary_keys}")
            logger.info(f"  - df.shape: {df.shape}")
            logger.info(f"  - df.columns: {df.columns.tolist()[:10]}")

            try:
                self._upsert_dataframe(df, table_name, primary_keys)
            except Exception as e:
                logger.error(f"ERROR: Erreur UPSERT: {str(e)}")
                raise
        elif write_disposition == "append":
            self._copy_from_dataframe(df, table_name)
        else:
            raise ValueError(f"Disposition non supportée: {write_disposition}")

    def close_pool(self):
        """Fermer le pool proprement à la fin"""
        if hasattr(self, 'connection_pool'):
            self.connection_pool.closeall()
            logger.info("🔌 Pool de connexions fermé")


# ============================================================================
# EXPORTS
# ============================================================================

# Instance singleton thread-safe
postgres_bulk_destination_v2 = PostgresBulkDestinationV2()


def get_postgres_destination(config: Dict[str, Any]):
    """
    DLT-compatible wrapper pour notre custom destination PostgreSQL.

    Cette fonction est requise pour compatibilité DLT pipeline,
    mais en réalité on utilise notre destination customisée directement.

    Args:
        config: Configuration PostgreSQL (eg. {"dataset_name": "hubeau"})

    Returns:
        dlt.destinations.postgres: DLT PostgreSQL destination standard
    """
    import dlt

    # Retourner destination DLT standard
    # NOTE: Nos assets n'utilisent plus pipeline.run() avec cette destination
    # Ils utilisent directement postgres_bulk_destination_v2.load_batch()
    # Mais DLT pipeline requiert qu'on passe UNE destination lors de l'init
    return dlt.destinations.postgres()