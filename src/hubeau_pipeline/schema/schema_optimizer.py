"""
SchemaOptimizer - Optimisation intelligente des schémas PostgreSQL

Transforme les tables "TEXT everywhere" en schémas optimisés avec:
- Types corrects (INTEGER, FLOAT, TIMESTAMP, BOOLEAN, GEOMETRY)
- Primary Keys détectées automatiquement
- Foreign Keys inférées depuis les patterns Hub'Eau
- Index sur colonnes clés et géospatiales (GIST)
- Support PostGIS pour coordonnées GPS

Approche en 2 phases:
1. Phase 1 (ce module): Ingestion ULTRA-SAFE tout en TEXT (zero error)
2. Phase 2 (post-processing): Optimisation intelligente du schéma

Avantages:
- Séparation ingestion (vitesse) / optimisation (qualité)
- Pas de retry loops pendant COPY
- Permet analyse statistique pour inférence précise
"""

import logging
import re
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass
import psycopg2

logger = logging.getLogger(__name__)


@dataclass
class ColumnTypeInfo:
    """Information sur le type inféré d'une colonne"""
    column_name: str
    current_type: str
    inferred_type: str
    nullable: bool
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_table: Optional[str] = None
    needs_index: bool = False
    sample_values: List[str] = None


@dataclass
class TableOptimization:
    """Plan d'optimisation pour une table"""
    table_name: str
    schema: str
    columns: List[ColumnTypeInfo]
    primary_keys: List[str]
    foreign_keys: Dict[str, Tuple[str, str]]  # col_name -> (ref_table, ref_column)
    indexes_to_create: List[Dict]
    has_geometry: bool = False


class SchemaOptimizer:
    """
    Optimiseur de schéma PostgreSQL intelligent

    Analyse les données réelles et infère:
    - Types optimaux (INTEGER, DOUBLE PRECISION, TIMESTAMP, BOOLEAN, GEOMETRY)
    - Primary Keys (patterns Hub'Eau: code_*, *_id)
    - Foreign Keys (code_station, code_commune, etc.)
    - Index nécessaires (PK, FK, spatial, frequently filtered)
    """

    # Patterns Hub'Eau pour Primary Keys
    PK_PATTERNS = [
        r'^code_',           # code_station, code_commune, code_bss
        r'_id$',             # station_id, observation_id
        r'^id$',             # id simple
    ]

    # Patterns pour Foreign Keys (column_name -> referenced_table_pattern)
    FK_PATTERNS = {
        'code_station': ['stations', 'station'],
        'code_commune': ['communes', 'commune', 'referentiel'],
        'code_departement': ['departements', 'departement', 'referentiel'],
        'code_bss': ['stations', 'piezometry_stations'],
        'code_cours_eau': ['cours_eau', 'referentiel'],
        'code_site': ['sites', 'hydrometry_sites'],
    }

    # Colonnes géospatiales (coord X/Y → GEOMETRY(Point, 4326))
    GEOMETRY_PATTERNS = {
        'longitude': 'longitude',
        'latitude': 'latitude',
        'coord_x': 'x',
        'coord_y': 'y',
        'coordonnee_x': 'x',
        'coordonnee_y': 'y',
    }

    def __init__(self, conn_params: Dict):
        """
        Args:
            conn_params: Paramètres de connexion PostgreSQL
        """
        self.conn_params = conn_params

    def _get_connection(self):
        """Créer une connexion PostgreSQL"""
        return psycopg2.connect(**self.conn_params)

    def _infer_column_type(self,
                          schema: str,
                          table: str,
                          column: str,
                          current_type: str,
                          conn) -> Tuple[str, bool]:
        """
        Infère le type optimal pour une colonne en analysant les données réelles

        Returns:
            (inferred_type, is_nullable)
        """
        # ✅ FIX #1: Colonnes qui doivent TOUJOURS rester en TEXT
        TEXT_ONLY_COLUMNS = [
            'code_commune_insee',  # Codes INSEE Corse: 2A004, 2B033, etc.
            'code_insee',
            'code_postal',         # Codes postaux avec zéros initiaux
            'numero_telephone',
            'numero_siret',
            'numero_siren',
            'code_bss'            # Peut avoir format alphanumérique
        ]

        if column.lower() in TEXT_ONLY_COLUMNS:
            logger.info(f"  ℹ️ Colonne {column} forcée en TEXT (codes alphanumériques)")
            # Vérifier juste la nullabilité
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(*) as total, COUNT({column}) as non_null
                    FROM {schema}.{table}
                """)
                total, non_null = cur.fetchone()
                is_nullable = (non_null < total)
                return 'TEXT', is_nullable

        with conn.cursor() as cur:
            # ✅ FIX #2: Détecter le type actuel avant comparaison avec ''
            cur.execute("""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s AND column_name = %s
            """, (schema, table, column))

            row = cur.fetchone()
            current_db_type = row[0] if row else 'text'

            # Adapter la condition WHERE selon le type actuel
            if current_db_type in ('integer', 'bigint', 'smallint', 'numeric', 'real', 'double precision'):
                # Types numériques: pas de comparaison avec ''
                where_clause = f"{column} IS NOT NULL"
            else:
                # Types texte: exclure NULL et chaînes vides
                where_clause = f"{column} IS NOT NULL AND {column}::text != ''"

            # 1. Statistiques de base
            cur.execute(f"""
                SELECT
                    COUNT(*) as total,
                    COUNT({column}) as non_null,
                    COUNT(DISTINCT {column}) as distinct_values
                FROM {schema}.{table}
                WHERE {where_clause}
            """)
            total, non_null, distinct = cur.fetchone()

            is_nullable = (non_null < total)

            if non_null == 0:
                return 'TEXT', True  # Pas de données, garder TEXT

            # 2. Échantillonner valeurs
            cur.execute(f"""
                SELECT {column}
                FROM {schema}.{table}
                WHERE {where_clause}
                LIMIT 100
            """)
            samples = [row[0] for row in cur.fetchall()]

            if not samples:
                return 'TEXT', True

            # 3. Détection de type basée sur les patterns

            # BOOLEAN (true/false, t/f, 1/0, oui/non)
            if self._is_boolean_column(samples):
                return 'BOOLEAN', is_nullable

            # INTEGER (nombres entiers)
            if self._is_integer_column(samples):
                # Choisir taille selon max value
                # ✅ FIX #7: cur.execute() ne retourne pas la valeur, il faut fetchone()
                cur.execute(f"""
                    SELECT MAX(ABS({column}::BIGINT))
                    FROM {schema}.{table}
                    WHERE {column} ~ '^-?[0-9]+$'
                """)
                max_val_result = cur.fetchone()
                if max_val_result and max_val_result[0]:
                    max_val = max_val_result[0]
                    if max_val < 32767:
                        return 'SMALLINT', is_nullable
                    elif max_val < 2147483647:
                        return 'INTEGER', is_nullable
                    else:
                        return 'BIGINT', is_nullable
                return 'INTEGER', is_nullable

            # DOUBLE PRECISION (nombres décimaux)
            if self._is_float_column(samples):
                return 'DOUBLE PRECISION', is_nullable

            # TIMESTAMP (dates/timestamps ISO ou epochs)
            if self._is_timestamp_column(samples):
                return 'TIMESTAMP', is_nullable

            # DATE (dates uniquement, pas d'heures)
            if self._is_date_column(samples):
                return 'DATE', is_nullable

            # Sinon, garder TEXT
            return 'TEXT', is_nullable

    def _is_boolean_column(self, samples: List[str]) -> bool:
        """Détecte si la colonne contient des booléens"""
        boolean_values = {
            'true', 'false', 't', 'f',
            '1', '0',
            'oui', 'non', 'yes', 'no',
            'vrai', 'faux'
        }
        unique_vals = {str(s).lower().strip() for s in samples}
        return unique_vals.issubset(boolean_values)

    def _is_integer_column(self, samples: List[str]) -> bool:
        """Détecte si la colonne contient des entiers"""
        try:
            for sample in samples:
                # Regex: optional minus, digits only
                if not re.match(r'^-?[0-9]+$', str(sample).strip()):
                    return False
            return True
        except:
            return False

    def _is_float_column(self, samples: List[str]) -> bool:
        """Détecte si la colonne contient des nombres décimaux"""
        try:
            for sample in samples:
                sample_clean = str(sample).strip().replace(',', '.')
                float(sample_clean)
            return True
        except:
            return False

    def _is_timestamp_column(self, samples: List[str]) -> bool:
        """Détecte si la colonne contient des timestamps"""
        # Patterns ISO: 2024-01-15T10:30:00, 2024-01-15 10:30:00, etc.
        timestamp_pattern = r'\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}'

        matches = 0
        for sample in samples[:20]:  # Check first 20
            if re.match(timestamp_pattern, str(sample).strip()):
                matches += 1

        return matches / min(len(samples), 20) > 0.8  # 80% match

    def _is_date_column(self, samples: List[str]) -> bool:
        """Détecte si la colonne contient des dates (sans heures)"""
        # Pattern: YYYY-MM-DD ou DD/MM/YYYY
        date_pattern = r'^(\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4})$'

        matches = 0
        for sample in samples[:20]:
            if re.match(date_pattern, str(sample).strip()):
                matches += 1

        return matches / min(len(samples), 20) > 0.8

    def _detect_primary_keys(self,
                            schema: str,
                            table: str,
                            columns: List[str],
                            conn) -> List[str]:
        """
        Détecte les Primary Keys candidates basées sur:
        1. Patterns de nommage Hub'Eau (code_*, *_id)
        2. Unicité des valeurs
        3. Non-nullité
        """
        pk_candidates = []

        with conn.cursor() as cur:
            for col in columns:
                # Check pattern matching
                matches_pattern = any(
                    re.search(pattern, col, re.IGNORECASE)
                    for pattern in self.PK_PATTERNS
                )

                if not matches_pattern:
                    continue

                # Check uniqueness and non-null
                cur.execute(f"""
                    SELECT
                        COUNT(*) as total,
                        COUNT(DISTINCT {col}) as distinct_count,
                        COUNT({col}) as non_null
                    FROM {schema}.{table}
                """)
                total, distinct, non_null = cur.fetchone()

                # Primary Key si: unique ET non-null
                if distinct == total and non_null == total and total > 0:
                    pk_candidates.append(col)
                    logger.info(f"  🔑 PK détectée: {col} ({total} valeurs uniques)")

        return pk_candidates

    def _detect_foreign_keys(self,
                            schema: str,
                            table: str,
                            columns: List[str],
                            conn) -> Dict[str, Tuple[str, str]]:
        """
        Détecte les Foreign Keys en:
        1. Matching patterns de nommage (code_station, code_commune, etc.)
        2. Vérification de l'existence des tables référencées
        3. Vérification que les valeurs existent dans la table cible
        """
        fks = {}

        with conn.cursor() as cur:
            # Lister toutes les tables du schéma
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
            """, (schema,))
            available_tables = {row[0] for row in cur.fetchall()}

            for col in columns:
                # Check FK patterns
                for fk_pattern, ref_table_patterns in self.FK_PATTERNS.items():
                    if col.lower().startswith(fk_pattern):
                        # Trouver table référencée
                        for pattern in ref_table_patterns:
                            matching_tables = [
                                t for t in available_tables
                                if pattern in t.lower()
                            ]

                            if matching_tables:
                                ref_table = matching_tables[0]
                                # Trouver colonne PK dans table référencée
                                # ✅ FIX #3: Corriger la précédence des opérateurs AND/OR avec parenthèses
                                cur.execute("""
                                    SELECT column_name
                                    FROM information_schema.columns
                                    WHERE table_schema = %s AND table_name = %s
                                    AND (column_name LIKE 'code_%%' OR column_name LIKE '%%_id')
                                    LIMIT 1
                                """, (schema, ref_table))
                                ref_col_result = cur.fetchone()

                                # ✅ FIX #4: Vérifier que le résultat existe avant d'accéder au tuple
                                if ref_col_result and len(ref_col_result) > 0:
                                    ref_col = ref_col_result[0]
                                    fks[col] = (ref_table, ref_col)
                                    logger.info(f"  🔗 FK détectée: {col} → {ref_table}.{ref_col}")
                                    break

        return fks

    def _detect_geometry_columns(self,
                                schema: str,
                                table: str,
                                columns: List[str],
                                conn) -> Optional[Dict]:
        """
        Détecte les colonnes de coordonnées GPS (longitude/latitude)
        et crée une colonne GEOMETRY

        Returns:
            Dict avec x_col, y_col si trouvé, sinon None
        """
        x_col = None
        y_col = None

        for col in columns:
            col_lower = col.lower()
            if col_lower in ['longitude', 'coord_x', 'coordonnee_x', 'x']:
                x_col = col
            elif col_lower in ['latitude', 'coord_y', 'coordonnee_y', 'y']:
                y_col = col

        if x_col and y_col:
            logger.info(f"  🌍 Coordonnées GPS détectées: {x_col}, {y_col}")
            return {'x_col': x_col, 'y_col': y_col}

        return None

    def analyze_table(self, schema: str, table: str) -> TableOptimization:
        """
        Analyse une table et génère un plan d'optimisation complet

        Returns:
            TableOptimization avec types inférés, PK/FK, et index à créer
        """
        logger.info(f"📊 Analyse de {schema}.{table}...")

        conn = self._get_connection()

        try:
            with conn.cursor() as cur:
                # 1. Récupérer colonnes actuelles
                cur.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema, table))

                columns_info = cur.fetchall()
                column_names = [c[0] for c in columns_info]

                # 2. Inférer types pour chaque colonne
                columns = []
                for col_name, current_type, is_nullable_str in columns_info:
                    inferred_type, nullable = self._infer_column_type(
                        schema, table, col_name, current_type, conn
                    )

                    columns.append(ColumnTypeInfo(
                        column_name=col_name,
                        current_type=current_type,
                        inferred_type=inferred_type,
                        nullable=nullable
                    ))

                # 3. Détecter Primary Keys
                pk_cols = self._detect_primary_keys(schema, table, column_names, conn)
                for col in columns:
                    if col.column_name in pk_cols:
                        col.is_primary_key = True

                # 4. Détecter Foreign Keys
                fks = self._detect_foreign_keys(schema, table, column_names, conn)
                for col in columns:
                    if col.column_name in fks:
                        col.is_foreign_key = True
                        col.foreign_table, ref_col = fks[col.column_name]
                        col.needs_index = True  # FK toujours indexée

                # 5. Détecter colonnes géométriques
                geom_info = self._detect_geometry_columns(schema, table, column_names, conn)

                # 6. Déterminer index à créer
                indexes = []

                # Index PK
                if pk_cols:
                    indexes.append({
                        'name': f'idx_{table}_pk',
                        'columns': pk_cols,
                        'type': 'btree',
                        'unique': True
                    })

                # Index FK
                for fk_col in fks.keys():
                    indexes.append({
                        'name': f'idx_{table}_{fk_col}',
                        'columns': [fk_col],
                        'type': 'btree',
                        'unique': False
                    })

                # Index spatial si géométrie
                if geom_info:
                    indexes.append({
                        'name': f'idx_{table}_geom',
                        'columns': ['geom'],
                        'type': 'gist',
                        'unique': False
                    })

                return TableOptimization(
                    table_name=table,
                    schema=schema,
                    columns=columns,
                    primary_keys=pk_cols,
                    foreign_keys=fks,
                    indexes_to_create=indexes,
                    has_geometry=geom_info is not None
                )

        finally:
            conn.close()

    def _validate_type_conversion(self, schema: str, table: str, column: str,
                                   target_type: str, conn) -> Tuple[bool, Optional[List[str]]]:
        """
        ✅ FIX #5: Valider AVANT ALTER TABLE pour éviter les erreurs

        Returns:
            (is_valid, example_invalid_values)
        """
        with conn.cursor() as cur:
            if target_type in ('INTEGER', 'SMALLINT', 'BIGINT'):
                # Vérifier que toutes les valeurs sont numériques
                cur.execute(f"""
                    SELECT COUNT(*)
                    FROM {schema}.{table}
                    WHERE {column} IS NOT NULL
                      AND {column}::text !~ '^-?[0-9]+$'
                """)
                invalid_count = cur.fetchone()[0]

                if invalid_count > 0:
                    # Récupérer exemples de valeurs invalides
                    cur.execute(f"""
                        SELECT array_agg(val)
                        FROM (
                            SELECT DISTINCT {column} as val
                            FROM {schema}.{table}
                            WHERE {column} IS NOT NULL
                              AND {column}::text !~ '^-?[0-9]+$'
                            ORDER BY val
                            LIMIT 5
                        ) sub
                    """)
                    examples = cur.fetchone()[0]
                    return False, examples

            elif target_type in ('DOUBLE PRECISION', 'NUMERIC', 'REAL'):
                # Vérifier que toutes les valeurs sont des nombres (entiers ou décimaux)
                cur.execute(f"""
                    SELECT COUNT(*)
                    FROM {schema}.{table}
                    WHERE {column} IS NOT NULL
                      AND {column}::text !~ '^-?[0-9]+(\\.[0-9]+)?$'
                """)
                invalid_count = cur.fetchone()[0]

                if invalid_count > 0:
                    # Récupérer exemples de valeurs invalides
                    cur.execute(f"""
                        SELECT array_agg(val)
                        FROM (
                            SELECT DISTINCT {column} as val
                            FROM {schema}.{table}
                            WHERE {column} IS NOT NULL
                              AND {column}::text !~ '^-?[0-9]+(\\.[0-9]+)?$'
                            ORDER BY val
                            LIMIT 5
                        ) sub
                    """)
                    examples = cur.fetchone()[0]
                    return False, examples

            elif target_type in ('TIMESTAMP', 'DATE'):
                # Vérifier que les valeurs peuvent être converties en date/timestamp
                try:
                    cur.execute(f"""
                        SELECT {column}::{target_type}
                        FROM {schema}.{table}
                        WHERE {column} IS NOT NULL
                        LIMIT 1
                    """)
                except Exception:
                    # Si même un seul échoue, on ne peut pas convertir
                    cur.execute(f"""
                        SELECT array_agg(val)
                        FROM (
                            SELECT DISTINCT {column} as val
                            FROM {schema}.{table}
                            WHERE {column} IS NOT NULL
                            ORDER BY val
                            LIMIT 5
                        ) sub
                    """)
                    examples = cur.fetchone()[0]
                    return False, examples

            return True, None

    def optimize_table(self, plan: TableOptimization, dry_run: bool = False) -> Dict:
        """
        Applique le plan d'optimisation à une table

        Args:
            plan: Plan d'optimisation généré par analyze_table()
            dry_run: Si True, affiche les commandes sans les exécuter

        Returns:
            Dict avec statistiques d'exécution
        """
        logger.info(f"🔧 Optimisation de {plan.schema}.{plan.table_name} (dry_run={dry_run})...")

        stats = {
            'types_changed': 0,
            'indexes_created': 0,
            'pk_created': False,
            'fks_created': 0,
            'geometry_created': False,
            'errors': []
        }

        conn = self._get_connection()

        try:
            with conn.cursor() as cur:
                # ✅ FIX #6: Utiliser SAVEPOINT pour rollback partiel en cas d'erreur
                if not dry_run:
                    cur.execute("SAVEPOINT before_optimization")

                # 1. ALTER types
                for col in plan.columns:
                    if col.current_type != col.inferred_type:
                        # ✅ FIX #5: Valider AVANT de tenter l'ALTER
                        is_valid, invalid_examples = self._validate_type_conversion(
                            plan.schema, plan.table_name, col.column_name,
                            col.inferred_type, conn
                        )

                        if not is_valid:
                            error_msg = (
                                f"Impossible de convertir {col.column_name} en {col.inferred_type}: "
                                f"valeurs incompatibles trouvées (ex: {invalid_examples[:3] if invalid_examples else 'N/A'})"
                            )
                            stats['errors'].append(error_msg)
                            logger.warning(f"  ⚠️ {error_msg}")
                            continue

                        alter_sql = f"""
                            ALTER TABLE {plan.schema}.{plan.table_name}
                            ALTER COLUMN {col.column_name} TYPE {col.inferred_type}
                            USING {col.column_name}::{col.inferred_type}
                        """

                        if dry_run:
                            logger.info(f"  [DRY] {alter_sql}")
                        else:
                            try:
                                cur.execute(alter_sql)
                                stats['types_changed'] += 1
                                logger.info(f"  ✅ {col.column_name}: {col.current_type} → {col.inferred_type}")
                            except Exception as e:
                                # Rollback to savepoint en cas d'erreur
                                cur.execute("ROLLBACK TO SAVEPOINT before_optimization")
                                cur.execute("SAVEPOINT before_optimization")

                                error_msg = f"Erreur ALTER {col.column_name}: {e}"
                                stats['errors'].append(error_msg)
                                logger.warning(f"  ⚠️ {error_msg}")

                # 2. CREATE Primary Key
                if plan.primary_keys:
                    pk_sql = f"""
                        ALTER TABLE {plan.schema}.{plan.table_name}
                        ADD CONSTRAINT {plan.table_name}_pkey
                        PRIMARY KEY ({', '.join(plan.primary_keys)})
                    """

                    if dry_run:
                        logger.info(f"  [DRY] {pk_sql}")
                    else:
                        try:
                            cur.execute(pk_sql)
                            stats['pk_created'] = True
                            logger.info(f"  ✅ PK créée: {', '.join(plan.primary_keys)}")
                        except Exception as e:
                            error_msg = f"Erreur PK: {e}"
                            stats['errors'].append(error_msg)
                            logger.warning(f"  ⚠️ {error_msg}")

                # 3. CREATE Foreign Keys
                for col, (ref_table, ref_col) in plan.foreign_keys.items():
                    fk_sql = f"""
                        ALTER TABLE {plan.schema}.{plan.table_name}
                        ADD CONSTRAINT fk_{plan.table_name}_{col}
                        FOREIGN KEY ({col})
                        REFERENCES {plan.schema}.{ref_table}({ref_col})
                        ON DELETE CASCADE
                    """

                    if dry_run:
                        logger.info(f"  [DRY] {fk_sql}")
                    else:
                        try:
                            cur.execute(fk_sql)
                            stats['fks_created'] += 1
                            logger.info(f"  ✅ FK créée: {col} → {ref_table}.{ref_col}")
                        except Exception as e:
                            error_msg = f"Erreur FK {col}: {e}"
                            stats['errors'].append(error_msg)
                            logger.warning(f"  ⚠️ {error_msg}")

                # 4. CREATE Indexes
                for idx in plan.indexes_to_create:
                    if idx['type'] == 'gist':
                        idx_sql = f"""
                            CREATE INDEX IF NOT EXISTS {idx['name']}
                            ON {plan.schema}.{plan.table_name}
                            USING GIST ({', '.join(idx['columns'])})
                        """
                    else:
                        unique = 'UNIQUE ' if idx['unique'] else ''
                        idx_sql = f"""
                            CREATE {unique}INDEX IF NOT EXISTS {idx['name']}
                            ON {plan.schema}.{plan.table_name} ({', '.join(idx['columns'])})
                        """

                    if dry_run:
                        logger.info(f"  [DRY] {idx_sql}")
                    else:
                        try:
                            cur.execute(idx_sql)
                            stats['indexes_created'] += 1
                            logger.info(f"  ✅ Index créé: {idx['name']}")
                        except Exception as e:
                            error_msg = f"Erreur index {idx['name']}: {e}"
                            stats['errors'].append(error_msg)
                            logger.warning(f"  ⚠️ {error_msg}")

                # Commit si pas dry_run
                if not dry_run:
                    # ✅ FIX #6: Release le savepoint si tout s'est bien passé
                    cur.execute("RELEASE SAVEPOINT before_optimization")
                    conn.commit()
                    logger.info(f"✅ Optimisation terminée: {stats}")

        finally:
            conn.close()

        return stats

    def optimize_schema(self, schema: str, tables: Optional[List[str]] = None, dry_run: bool = False):
        """
        Optimise toutes les tables d'un schéma (ou liste spécifique)

        Args:
            schema: Nom du schéma PostgreSQL
            tables: Liste de tables à optimiser (None = toutes)
            dry_run: Mode simulation
        """
        conn = self._get_connection()

        try:
            with conn.cursor() as cur:
                if tables is None:
                    # Récupérer toutes les tables du schéma
                    cur.execute("""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = %s AND table_type = 'BASE TABLE'
                        ORDER BY table_name
                    """, (schema,))
                    tables = [row[0] for row in cur.fetchall()]
        finally:
            conn.close()

        logger.info(f"🚀 Optimisation de {len(tables)} tables dans {schema}")

        results = {}
        for table in tables:
            try:
                # Analyse
                plan = self.analyze_table(schema, table)

                # Optimisation
                stats = self.optimize_table(plan, dry_run=dry_run)
                results[table] = stats

            except Exception as e:
                logger.error(f"❌ Erreur sur {table}: {e}")
                results[table] = {'error': str(e)}

        return results
