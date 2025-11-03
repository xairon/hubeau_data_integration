"""
Script pour générer les fichiers SQL de schéma avec les bons types PostgreSQL.
Applique les COLUMN_TYPE_OVERRIDES pour corriger les types des FK numériques.

Usage:
    python scripts/generate_schema_sql.py

Output:
    Génère/écrase tous les fichiers scripts/schema/*.sql
"""

import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from hubeau_pipeline.schema import (
    HUBEAU_FIELD_TYPES,
    TABLE_PK_MAPPING,
    TABLE_FK_MAPPING,
    COLUMN_TYPE_OVERRIDES,
    get_primary_key,
    get_foreign_keys,
)


def apply_type_overrides(table_name: str, column_name: str, original_type: str) -> str:
    """
    Applique les overrides de types pour une colonne.

    Args:
        table_name: Nom de la table
        column_name: Nom de la colonne
        original_type: Type original (depuis HUBEAU_FIELD_TYPES)

    Returns:
        Type final à utiliser (avec override si applicable)
    """
    # Check if column has a type override
    if column_name in COLUMN_TYPE_OVERRIDES:
        override_type = COLUMN_TYPE_OVERRIDES[column_name]
        if override_type != original_type:
            print(f"  [OVERRIDE] {table_name}.{column_name}: {original_type} -> {override_type}")
        return override_type

    return original_type


def generate_table_sql(table_name: str) -> str:
    """
    Génère le SQL CREATE TABLE pour une table Hub'Eau.

    Args:
        table_name: Nom de la table

    Returns:
        SQL complet avec CREATE TABLE, PK, FK, indexes
    """
    schema_def = HUBEAU_FIELD_TYPES.get(table_name)
    if not schema_def:
        raise ValueError(f"Table {table_name} not found in HUBEAU_FIELD_TYPES")

    primary_keys = get_primary_key(table_name)
    foreign_keys = get_foreign_keys(table_name)

    # Header
    sql = f"""-- Table: {table_name}
-- Source: Hub'Eau API
-- PRIMARY KEY: {', '.join(primary_keys) if primary_keys else 'None'}

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.{table_name} (
"""

    # Columns with type overrides
    column_defs = []
    for column_name, original_type in schema_def.items():
        # Apply type override if applicable
        pg_type = apply_type_overrides(table_name, column_name, original_type)

        # Add NOT NULL constraint for PK columns
        not_null = " NOT NULL" if column_name in primary_keys else ""

        column_defs.append(f"    {column_name} {pg_type}{not_null}")

    sql += ",\n".join(column_defs)

    # Primary Key constraint
    if primary_keys:
        sql += f",\n    PRIMARY KEY ({', '.join(primary_keys)})"

    sql += "\n);\n"

    # Foreign Keys
    if foreign_keys:
        sql += "\n"
        for local_col, ref_table, ref_col in foreign_keys:
            constraint_name = f"fk_{table_name}_{local_col}"
            sql += f"""-- Foreign Key: {local_col} -> {ref_table}.{ref_col}
ALTER TABLE hubeau.{table_name} DROP CONSTRAINT IF EXISTS {constraint_name};
ALTER TABLE hubeau.{table_name}
ADD CONSTRAINT {constraint_name}
FOREIGN KEY ({local_col}) REFERENCES hubeau.{ref_table}({ref_col})
ON DELETE CASCADE;

"""

    # Indexes on date columns (for partitioning/filtering)
    date_columns = [col for col, typ in schema_def.items() if "TIMESTAMP" in typ or "DATE" in typ]
    if date_columns:
        sql += "-- Index temporels\n"
        for date_col in date_columns:
            idx_name = f"idx_{table_name}_{date_col}"
            sql += f"CREATE INDEX IF NOT EXISTS {idx_name}\nON hubeau.{table_name}({date_col});\n\n"

    # Table comment
    pk_comment = f"PRIMARY KEY: {', '.join(primary_keys)}" if primary_keys else "No PRIMARY KEY"
    sql += f"""COMMENT ON TABLE hubeau.{table_name} IS
'Table Hub''Eau: {table_name} - {pk_comment}';
"""

    return sql


def main():
    """Génère tous les fichiers SQL de schéma."""
    output_dir = project_root / "scripts" / "schema"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[*] Generation des schemas SQL dans {output_dir}/")
    print(f"[*] {len(HUBEAU_FIELD_TYPES)} tables a generer")
    print(f"[*] {len(COLUMN_TYPE_OVERRIDES)} overrides de types definis\n")

    for table_name in sorted(HUBEAU_FIELD_TYPES.keys()):
        print(f"Generating {table_name}.sql...")

        sql_content = generate_table_sql(table_name)

        output_path = output_dir / f"{table_name}.sql"
        output_path.write_text(sql_content, encoding="utf-8")

        print(f"  [OK] {output_path}\n")

    print(f"\n[OK] Generation terminee: {len(HUBEAU_FIELD_TYPES)} fichiers SQL crees")
    print(f"\n[INFO] Pour appliquer les nouveaux schemas:")
    print(f"   1. DROP toutes les tables Hub'Eau (ou ALTER COLUMN)")
    print(f"   2. Rebuild Docker images: docker compose build")
    print(f"   3. Restart containers: docker compose up -d")
    print(f"   4. Dagster recreera les tables automatiquement")


if __name__ == "__main__":
    main()
