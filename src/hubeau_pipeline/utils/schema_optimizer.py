"""
Schema Optimizer - Optimise les types de colonnes après ingestion

Stratégie:
1. Ingestion initiale: TOUT en TEXT (safe, rapide, zéro erreur)
2. Post-processing: Analyse des données + conversion vers types optimaux
3. Résultat: Meilleure performance queries + stockage réduit
"""

import psycopg2
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class SchemaOptimizer:
    """Optimise les types de colonnes PostgreSQL après ingestion"""

    def __init__(self, conn_params: Dict[str, str], schema_name: str = "hubeau"):
        self.conn_params = conn_params
        self.schema_name = schema_name

    def optimize_table(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Analyse et optimise les types de colonnes d'une table

        Returns:
            Liste des optimisations appliquées
        """
        logger.info(f"🔍 Analyse de {self.schema_name}.{table_name} pour optimisation...")

        conn = psycopg2.connect(**self.conn_params)
        try:
            optimizations = []

            # 1. Récupérer les colonnes TEXT à analyser
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = %s
                    AND table_name = %s
                    AND data_type IN ('text', 'character varying')
                    ORDER BY ordinal_position
                """, (self.schema_name, table_name))

                text_columns = [row[0] for row in cursor.fetchall()]

            if not text_columns:
                logger.info(f"✅ {table_name}: Aucune colonne TEXT à optimiser")
                return []

            logger.info(f"📊 {len(text_columns)} colonnes TEXT à analyser")

            # 2. Analyser chaque colonne
            for col_name in text_columns:
                opt = self._analyze_column(conn, table_name, col_name)
                if opt:
                    optimizations.append(opt)

            # 3. Appliquer les optimisations
            applied = []
            for opt in optimizations:
                if self._apply_optimization(conn, table_name, opt):
                    applied.append(opt)

            conn.commit()
            logger.info(f"✅ {table_name}: {len(applied)}/{len(optimizations)} optimisations appliquées")

            return applied

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Erreur lors de l'optimisation de {table_name}: {e}")
            raise
        finally:
            conn.close()

    def _analyze_column(self, conn, table_name: str, col_name: str) -> Optional[Dict[str, Any]]:
        """Analyse une colonne et détermine le type optimal"""

        with conn.cursor() as cursor:
            # Statistiques sur la colonne
            cursor.execute(f"""
                SELECT
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT "{col_name}") as distinct_values,
                    COUNT("{col_name}") as non_null_count,
                    MAX(LENGTH("{col_name}"::text)) as max_length,
                    MIN(LENGTH("{col_name}"::text)) as min_length,
                    -- Test si 100% numeric
                    COUNT(CASE WHEN "{col_name}" ~ '^-?[0-9]+\.?[0-9]*$' THEN 1 END) as numeric_like,
                    -- Test si 100% integer
                    COUNT(CASE WHEN "{col_name}" ~ '^-?[0-9]+$' THEN 1 END) as integer_like,
                    -- Test si timestamp
                    COUNT(CASE WHEN "{col_name}" ~ '^\d{{4}}-\d{{2}}-\d{{2}}' THEN 1 END) as timestamp_like
                FROM {self.schema_name}.{table_name}
                WHERE "{col_name}" IS NOT NULL AND "{col_name}" != ''
            """)

            row = cursor.fetchone()
            if not row or row[0] == 0:
                return None

            total, distinct, non_null, max_len, min_len, numeric_like, integer_like, timestamp_like = row

            # Pas assez de données pour décider
            if non_null < 100:
                return None

            # Ratio de valeurs matchant le pattern
            numeric_ratio = numeric_like / non_null if non_null > 0 else 0
            integer_ratio = integer_like / non_null if non_null > 0 else 0
            timestamp_ratio = timestamp_like / non_null if non_null > 0 else 0

        # Décision du type optimal

        # 1. Timestamps (>95% match pattern)
        if timestamp_ratio > 0.95 and any(kw in col_name.lower() for kw in ['date', 'timestamp', 'time']):
            return {
                'column': col_name,
                'current_type': 'TEXT',
                'new_type': 'TIMESTAMP',
                'reason': f'{timestamp_ratio:.1%} timestamp-like values',
                'confidence': timestamp_ratio
            }

        # 2. Integers (>99% match pattern)
        if integer_ratio > 0.99:
            # Déterminer la taille optimale
            cursor.execute(f"""
                SELECT
                    MIN("{col_name}"::bigint) as min_val,
                    MAX("{col_name}"::bigint) as max_val
                FROM {self.schema_name}.{table_name}
                WHERE "{col_name}" ~ '^-?[0-9]+$'
            """)
            min_val, max_val = cursor.fetchone()

            if min_val is not None and max_val is not None:
                if min_val >= -32768 and max_val <= 32767:
                    int_type = 'SMALLINT'
                elif min_val >= -2147483648 and max_val <= 2147483647:
                    int_type = 'INTEGER'
                else:
                    int_type = 'BIGINT'

                return {
                    'column': col_name,
                    'current_type': 'TEXT',
                    'new_type': int_type,
                    'reason': f'{integer_ratio:.1%} integers (range: {min_val} to {max_val})',
                    'confidence': integer_ratio
                }

        # 3. Numerics (>99% match pattern)
        elif numeric_ratio > 0.99:
            # Coordonnées GPS
            if col_name.lower() in ['latitude', 'lat']:
                return {
                    'column': col_name,
                    'current_type': 'TEXT',
                    'new_type': 'DOUBLE PRECISION',
                    'reason': 'GPS latitude coordinate',
                    'confidence': 1.0
                }
            elif col_name.lower() in ['longitude', 'lon', 'lng']:
                return {
                    'column': col_name,
                    'current_type': 'TEXT',
                    'new_type': 'DOUBLE PRECISION',
                    'reason': 'GPS longitude coordinate',
                    'confidence': 1.0
                }
            else:
                return {
                    'column': col_name,
                    'current_type': 'TEXT',
                    'new_type': 'NUMERIC',
                    'reason': f'{numeric_ratio:.1%} numeric values',
                    'confidence': numeric_ratio
                }

        # 4. VARCHAR pour texte court et répétitif
        elif max_len and max_len < 255:
            distinct_ratio = distinct / non_null if non_null > 0 else 1.0

            # Beaucoup de répétitions (codes, catégories)
            if distinct_ratio < 0.5:
                return {
                    'column': col_name,
                    'current_type': 'TEXT',
                    'new_type': f'VARCHAR({int(max_len * 1.2)})',  # 20% marge
                    'reason': f'Short repeating text (max {max_len} chars, {distinct_ratio:.1%} distinct)',
                    'confidence': 0.8
                }

        # Pas d'optimisation trouvée
        return None

    def _apply_optimization(self, conn, table_name: str, opt: Dict[str, Any]) -> bool:
        """Applique une optimisation de type"""

        try:
            with conn.cursor() as cursor:
                # Conversion avec USING pour gérer les cas limites
                alter_sql = f"""
                    ALTER TABLE {self.schema_name}.{table_name}
                    ALTER COLUMN "{opt['column']}" TYPE {opt['new_type']}
                    USING NULLIF("{opt['column']}", '')::{opt['new_type']}
                """

                cursor.execute(alter_sql)
                logger.info(f"  ✅ {opt['column']}: {opt['current_type']} → {opt['new_type']} ({opt['reason']})")
                return True

        except Exception as e:
            logger.warning(f"  ⚠️ {opt['column']}: Cannot convert to {opt['new_type']}: {e}")
            return False


def optimize_all_csv_tables(conn_params: Dict[str, str], schema_name: str = "hubeau"):
    """
    Optimise toutes les tables CSV du schéma
    À lancer APRÈS le chargement initial de toutes les données
    """
    optimizer = SchemaOptimizer(conn_params, schema_name)

    # Liste des tables CSV à optimiser
    csv_tables = [
        "ecoulement_campagnes",
        "ecoulement_stations",
        "hydrobio_stations",
        "hydrometry_sites",
        "hydrometry_stations",
        "piezometry_stations",
        "prelevements_ouvrages",
        "prelevements_points",
        "quality_groundwater_stations",
        "quality_rivers_stations",
        "temperature_stations",
    ]

    results = {}
    for table in csv_tables:
        try:
            optimizations = optimizer.optimize_table(table)
            results[table] = optimizations
        except Exception as e:
            logger.error(f"❌ Erreur {table}: {e}")
            results[table] = []

    # Résumé
    total_optimizations = sum(len(opts) for opts in results.values())
    logger.info(f"\n🎉 Optimisation terminée: {total_optimizations} colonnes optimisées sur {len(csv_tables)} tables")

    return results
