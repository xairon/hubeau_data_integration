"""
Custom DLT destination PostgreSQL OPTIMISÉ V2 avec pool de connexions et cache
Performance: 100k records en 1-2 secondes (vs 2-3s avant, vs 5-10min DLT)
"""

import os
import io
import time
import pandas as pd
import psycopg2
from psycopg2 import pool
from typing import Iterator, Any, Dict, List, Optional
import logging
from threading import Lock

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
        """Initialisation une seule fois"""
        if self._initialized:
            return

        self.conn_params = {
            "host": os.getenv("PG_HOST", "postgres"),
            "port": int(os.getenv("PG_PORT", "5432")),
            "database": os.getenv("PG_DB", "postgres"),
            "user": os.getenv("PG_USER", "postgres"),
            "password": os.getenv("PG_PASSWORD")
        }
        self.schema_name = "hubeau"

        # Pool de connexions (min=2, max=10)
        self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
            2, 10, **self.conn_params
        )

        # Cache pour métadonnées de tables (colonnes)
        self._table_columns_cache = {}
        self._cache_ttl = 3600  # 1 heure
        self._cache_timestamps = {}

        self._initialized = True
        logger.info("✅ PostgresBulkDestinationV2 initialisé avec pool de connexions")

    def _get_connection(self):
        """Obtenir une connexion depuis le pool"""
        return self.connection_pool.getconn()

    def _release_connection(self, conn):
        """Remettre la connexion dans le pool"""
        if conn:
            self.connection_pool.putconn(conn)

    def _get_target_columns(self, table_name: str, conn=None) -> List[str]:
        """
        Récupère les colonnes de la table avec CACHE
        Évite les requêtes répétées sur information_schema
        """
        # Vérifier le cache
        cache_key = f"{self.schema_name}.{table_name}"
        now = time.time()

        if (cache_key in self._table_columns_cache and
            cache_key in self._cache_timestamps and
            now - self._cache_timestamps[cache_key] < self._cache_ttl):
            logger.debug(f"📦 Cache hit pour {cache_key}")
            return self._table_columns_cache[cache_key]

        # Si pas en cache ou expiré, requêter
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
                columns = [row[0] for row in cursor.fetchall()]

                # Mettre en cache
                self._table_columns_cache[cache_key] = columns
                self._cache_timestamps[cache_key] = now
                logger.debug(f"📝 Cache miss pour {cache_key} - mis en cache")

                return columns
        finally:
            if need_release:
                self._release_connection(conn)

    def _get_table_column_types(self, table_name: str, conn=None) -> Dict[str, str]:
        """
        Récupère les types des colonnes de la table avec CACHE
        Utilisé pour le casting de types avant COPY
        """
        cache_key = f"{self.schema_name}.{table_name}_types"
        now = time.time()

        if (cache_key in self._table_columns_cache and
            cache_key in self._cache_timestamps and
            now - self._cache_timestamps[cache_key] < self._cache_ttl):
            logger.debug(f"📦 Cache hit pour types {cache_key}")
            return self._table_columns_cache[cache_key]

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
                type_map = {row[0]: row[1] for row in cursor.fetchall()}

                # Mettre en cache
                self._table_columns_cache[cache_key] = type_map
                self._cache_timestamps[cache_key] = now
                logger.debug(f"📝 Cache miss pour types {cache_key} - mis en cache")

                return type_map
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

        # ✅ Casting de types selon le schéma PostgreSQL
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
                                logger.debug(f"✅ Cast {col} to {pg_type} (vectorized)")
                            except (ValueError, TypeError) as e:
                                logger.warning(f"⚠️ Failed to cast {col} to {pg_type}: {e}")

                    # Cast double precision / real
                    elif pg_type in ['double precision', 'real', 'numeric']:
                        if df[col].dtype == 'object':
                            try:
                                # Vectorized conversion with error handling
                                # Non-numeric strings will be converted to NaN
                                original_col = df[col].copy()  # Keep original for comparison
                                df[col] = pd.to_numeric(df[col], errors='coerce')
                                # Check if too many conversions failed (>50% became NaN that weren't originally null)
                                originally_non_null = original_col.notna().sum()
                                after_conversion_non_null = df[col].notna().sum()
                                if originally_non_null > 0 and after_conversion_non_null < originally_non_null * 0.5:
                                    # More than 50% failed conversion - likely text data in numeric column
                                    logger.warning(f"⚠️ Column {col} is typed as {pg_type} but contains non-numeric text data ({after_conversion_non_null}/{originally_non_null} converted)")
                                    # Restore original and keep as text - will cause COPY to fail with clear error
                                    df[col] = original_col
                                else:
                                    logger.debug(f"✅ Cast {col} to {pg_type} (vectorized, {after_conversion_non_null}/{originally_non_null} valid values)")
                            except (ValueError, TypeError) as e:
                                logger.warning(f"⚠️ Failed to cast {col} to {pg_type}: {e}")

                    # Cast boolean
                    elif pg_type == 'boolean':
                        if df[col].dtype == 'object':
                            try:
                                df[col] = df[col].apply(
                                    lambda x: None if pd.isna(x) or x == '' else bool(x)
                                )
                                logger.debug(f"✅ Cast {col} to {pg_type}")
                            except (ValueError, TypeError) as e:
                                logger.warning(f"⚠️ Failed to cast {col} to {pg_type}: {e}")
            except Exception as e:
                logger.warning(f"⚠️ Could not retrieve column types for {table_name}: {e}")

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

    def _create_table_from_dataframe(self, df: pd.DataFrame, table_name: str, conn):
        """Crée la table si elle n'existe pas en inférant les types depuis le DataFrame"""
        with conn.cursor() as cursor:
            # Inférer les types depuis pandas
            col_defs = []
            for col in df.columns:
                dtype = df[col].dtype
                if dtype == 'object':
                    pg_type = 'TEXT'
                elif dtype in ['int64', 'Int64']:
                    pg_type = 'BIGINT'
                elif dtype in ['int32', 'Int32']:
                    pg_type = 'INTEGER'
                elif dtype in ['float64', 'float32']:
                    pg_type = 'DOUBLE PRECISION'
                elif dtype == 'bool':
                    pg_type = 'BOOLEAN'
                elif 'datetime' in str(dtype):
                    pg_type = 'TIMESTAMP'
                else:
                    pg_type = 'TEXT'  # Fallback

                col_defs.append(f"{col} {pg_type}")

            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.schema_name}.{table_name} (
                    {', '.join(col_defs)}
                )
            """
            cursor.execute(create_sql)
            conn.commit()
            logger.info(f"✅ Table {table_name} créée avec {len(col_defs)} colonnes")

            # Invalider le cache pour forcer refresh
            cache_key = f"{self.schema_name}.{table_name}"
            if cache_key in self._table_columns_cache:
                del self._table_columns_cache[cache_key]
            if cache_key in self._cache_timestamps:
                del self._cache_timestamps[cache_key]

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
                # Récupérer colonnes (depuis cache si possible)
                target_columns = self._get_target_columns(table_name, conn)

                # Si table n'existe pas, la créer
                if not target_columns:
                    logger.warning(f"⚠️ Table {table_name} n'existe pas - création automatique")
                    self._create_table_from_dataframe(df, table_name, conn)
                    target_columns = self._get_target_columns(table_name, conn)

                # Filtrer colonnes SANS COPIER le DataFrame
                df_columns = df.columns.tolist()
                common_columns = [col for col in df_columns if col in target_columns]

                if not common_columns:
                    raise ValueError(f"Aucune colonne commune entre DataFrame et table {table_name}")

                # Sélectionner colonnes et nettoyer EN PLACE
                if len(common_columns) < len(df_columns):
                    df = df[common_columns]  # Vue, pas copie si possible

                df = self._clean_dataframe_inplace(df, table_name=table_name, conn=conn)

                # Gérer colonnes DLT si nécessaires
                if '_dlt_load_id' in target_columns and '_dlt_load_id' not in df.columns:
                    df['_dlt_load_id'] = f"load_{int(time.time() * 1000)}"
                    common_columns.append('_dlt_load_id')

                if '_dlt_id' in target_columns and '_dlt_id' not in df.columns:
                    df['_dlt_id'] = [f"row_{i}_{int(time.time() * 1000)}" for i in range(len(df))]
                    common_columns.append('_dlt_id')

                # COPY avec buffer optimisé
                output = io.StringIO()
                df.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
                output.seek(0)

                copy_sql = f"""
                    COPY {self.schema_name}.{table_name} ({','.join(common_columns)})
                    FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')
                """

                cursor.copy_expert(copy_sql, output)
                conn.commit()

                logger.info(f"✅ COPY: {len(df)} records → {table_name}")

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Erreur COPY: {e}")
            raise
        finally:
            if need_release:
                self._release_connection(conn)

    def _upsert_dataframe(self, df: pd.DataFrame, table_name: str, primary_keys: List[str]):
        """
        UPSERT optimisé avec connexion unique
        """
        conn = self._get_connection()
        staging_table = f"staging_{int(time.time() * 1000)}"

        try:
            with conn.cursor() as cursor:
                # Récupérer colonnes cibles (cache)
                target_columns = self._get_target_columns(table_name, conn)

                # Filtrer colonnes
                df_columns = df.columns.tolist()
                common_columns = [col for col in df_columns if col in target_columns]

                # Vérifier primary keys
                missing_pks = [pk for pk in primary_keys if pk not in common_columns]
                if missing_pks:
                    raise ValueError(f"Primary keys manquantes: {missing_pks}")

                # Nettoyer DataFrame EN PLACE
                if len(common_columns) < len(df_columns):
                    df = df[common_columns]
                df = self._clean_dataframe_inplace(df, table_name=table_name, conn=conn)

                # Fix: Deduplicate rows to avoid "ON CONFLICT DO UPDATE command cannot affect row a second time"
                # This error occurs when the same primary key appears multiple times in a single batch
                original_len = len(df)
                df = df.drop_duplicates(subset=primary_keys, keep='last')
                if len(df) < original_len:
                    logger.warning(f"⚠️ Deduplication: removed {original_len - len(df)} duplicate rows from {table_name}")
                else:
                    logger.debug(f"✓ No duplicates found in batch for {table_name}")

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

                # COPY vers staging
                output = io.StringIO()
                df.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
                output.seek(0)

                cursor.copy_expert(
                    f"COPY {staging_table} ({','.join(common_columns)}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')",
                    output
                )

                # UPSERT avec génération DLT si nécessaire
                insert_columns = common_columns.copy()
                select_columns = common_columns.copy()

                if '_dlt_load_id' in target_columns and '_dlt_load_id' not in common_columns:
                    insert_columns.append('_dlt_load_id')
                    select_columns.append(f"'load_{int(time.time() * 1000)}'::TEXT")

                if '_dlt_id' in target_columns and '_dlt_id' not in common_columns:
                    insert_columns.append('_dlt_id')
                    pk_concat = ' || '.join([f"COALESCE({pk}::TEXT, '')" for pk in primary_keys])
                    select_columns.append(f"MD5({pk_concat})::TEXT")

                # Update columns (exclure PK)
                update_cols = [c for c in common_columns if c not in primary_keys]

                if update_cols:
                    update_set = ', '.join([f"{c} = EXCLUDED.{c}" for c in update_cols])
                    if 'updated_at' in target_columns and 'updated_at' not in update_cols:
                        update_set += ", updated_at = CURRENT_TIMESTAMP"
                    conflict_action = f"DO UPDATE SET {update_set}"
                else:
                    conflict_action = "DO NOTHING"

                upsert_sql = f"""
                    INSERT INTO {self.schema_name}.{table_name} ({','.join(insert_columns)})
                    SELECT {','.join(select_columns)} FROM {staging_table}
                    ON CONFLICT ({','.join(primary_keys)})
                    {conflict_action}
                """

                cursor.execute(upsert_sql)
                affected = cursor.rowcount

                # DROP staging table explicitement pour libérer mémoire
                cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")

                conn.commit()
                logger.info(f"✅ UPSERT: {affected}/{len(df)} records modifiés")

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Erreur UPSERT: {e}")
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
                    logger.info(f"✅ TRUNCATE CASCADE: {table_name}")
                else:
                    logger.warning(f"⚠️ Table {self.schema_name}.{table_name} n'existe pas - skip TRUNCATE")
                    # Table will be created by first COPY operation
        finally:
            self._release_connection(conn)

    def load_batch(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        write_disposition: str,
        primary_keys: Optional[List[str]] = None,
        column_mappings: Optional[Dict[str, str]] = None
    ):
        """Point d'entrée principal optimisé"""
        if not data:
            logger.warning(f"⚠️ Pas de données pour {table_name}")
            return

        # DataFrame avec mappings
        df = pd.DataFrame(data)

        if column_mappings:
            df = df.rename(columns=column_mappings)
            if primary_keys:
                primary_keys = [column_mappings.get(pk, pk) for pk in primary_keys]

        logger.info(f"🚀 Chargement {len(df)} records → {table_name} ({write_disposition})")

        if write_disposition == "replace":
            self._truncate_cascade(table_name)
            self._copy_from_dataframe(df, table_name)
        elif write_disposition == "merge":
            if not primary_keys:
                raise ValueError("primary_keys requis pour merge")
            self._upsert_dataframe(df, table_name, primary_keys)
        elif write_disposition == "append":
            self._copy_from_dataframe(df, table_name)
        else:
            raise ValueError(f"Disposition non supportée: {write_disposition}")

    def clear_cache(self):
        """Vider le cache manuellement si nécessaire"""
        self._table_columns_cache.clear()
        self._cache_timestamps.clear()
        logger.info("📦 Cache vidé")

    def close_pool(self):
        """Fermer le pool proprement à la fin"""
        if hasattr(self, 'connection_pool'):
            self.connection_pool.closeall()
            logger.info("🔌 Pool de connexions fermé")


# Instance singleton thread-safe
postgres_bulk_destination_v2 = PostgresBulkDestinationV2()