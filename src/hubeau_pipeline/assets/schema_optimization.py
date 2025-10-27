"""
Assets Dagster pour l'optimisation des schémas PostgreSQL

Ces assets analysent et optimisent les tables Hub'Eau après ingestion:
- Conversion TEXT → types optimaux (INTEGER, FLOAT, TIMESTAMP, GEOMETRY)
- Création automatique de Primary Keys
- Détection et création de Foreign Keys
- Index sur colonnes clés et géospatiales
"""

from typing import Dict, Any
from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
    Config,
)
from pydantic import Field

from hubeau_pipeline.resources import PostgreSQLResource
from hubeau_pipeline.schema import SchemaOptimizer


class SchemaOptimizationConfig(Config):
    """Configuration pour l'optimisation du schéma"""
    schema: str = Field(default="hubeau", description="Schéma PostgreSQL à optimiser")
    tables: list[str] | None = Field(default=None, description="Liste des tables (None = toutes)")
    dry_run: bool = Field(default=False, description="Mode simulation sans modifications")


@asset(
    group_name="schema_management",
    description="Optimise les schémas PostgreSQL Hub'Eau (types, PK, FK, index)",
)
def optimize_hubeau_schema(
    context: AssetExecutionContext,
    config: SchemaOptimizationConfig,
    pg: PostgreSQLResource
) -> Output[Dict[str, Any]]:
    """
    Optimise toutes les tables Hub'Eau:
    - Inférence de types (TEXT → INTEGER/FLOAT/TIMESTAMP/BOOLEAN/GEOMETRY)
    - Détection automatique de Primary Keys (code_*, *_id)
    - Détection de Foreign Keys (code_station, code_commune, etc.)
    - Création d'index (PK, FK, spatial GIST)

    Stratégie 2-phases:
    1. Ingestion: tout en TEXT (ultra-safe, zéro error)
    2. Post-processing: optimisation intelligente (ce module)
    """
    context.log.info(f"🔧 Début optimisation schéma: {config.schema}")

    # Créer optimizer avec connexion PostgreSQL
    optimizer = SchemaOptimizer(conn_params={
        'host': pg.host,
        'port': pg.port,
        'database': pg.database,
        'user': pg.user,
        'password': pg.password
    })

    # Optimiser toutes les tables (ou liste spécifique)
    results = optimizer.optimize_schema(
        schema=config.schema,
        tables=config.tables,
        dry_run=config.dry_run
    )

    # Calculer statistiques globales
    total_tables = len(results)
    tables_with_errors = sum(1 for r in results.values() if 'error' in r)
    total_types_changed = sum(r.get('types_changed', 0) for r in results.values())
    total_indexes_created = sum(r.get('indexes_created', 0) for r in results.values())
    total_pks_created = sum(1 for r in results.values() if r.get('pk_created', False))
    total_fks_created = sum(r.get('fks_created', 0) for r in results.values())

    # Métadonnées pour Dagster UI
    metadata = {
        "total_tables": MetadataValue.int(total_tables),
        "tables_with_errors": MetadataValue.int(tables_with_errors),
        "types_changed": MetadataValue.int(total_types_changed),
        "indexes_created": MetadataValue.int(total_indexes_created),
        "primary_keys_created": MetadataValue.int(total_pks_created),
        "foreign_keys_created": MetadataValue.int(total_fks_created),
        "dry_run": MetadataValue.bool(config.dry_run),
        "results_detail": MetadataValue.json(results),
    }

    context.log.info(f"✅ Optimisation terminée:")
    context.log.info(f"   - {total_tables} tables traitées")
    context.log.info(f"   - {total_types_changed} types optimisés")
    context.log.info(f"   - {total_indexes_created} index créés")
    context.log.info(f"   - {total_pks_created} PK créées")
    context.log.info(f"   - {total_fks_created} FK créées")

    if tables_with_errors > 0:
        context.log.warning(f"⚠️ {tables_with_errors} tables avec erreurs")

    return Output(
        value={
            "schema": config.schema,
            "tables_optimized": total_tables,
            "types_changed": total_types_changed,
            "indexes_created": total_indexes_created,
            "primary_keys_created": total_pks_created,
            "foreign_keys_created": total_fks_created,
            "results": results
        },
        metadata=metadata
    )


class AnalyzeTableConfig(Config):
    """Configuration pour l'analyse d'une table"""
    schema: str = Field(default="hubeau", description="Schéma PostgreSQL")
    table: str = Field(default="piezometry_stations", description="Nom de la table à analyser")


@asset(
    group_name="schema_management",
    description="Analyse une table spécifique et génère le plan d'optimisation (sans l'appliquer)",
)
def analyze_table_schema(
    context: AssetExecutionContext,
    config: AnalyzeTableConfig,
    pg: PostgreSQLResource
) -> Output[Dict[str, Any]]:
    """
    Asset de test pour analyser une table sans modifications

    Utile pour:
    - Valider l'inférence de types
    - Voir les PK/FK détectées
    - Comprendre les optimisations proposées
    """
    schema = config.schema
    table = config.table

    context.log.info(f"📊 Analyse de {schema}.{table}...")

    optimizer = SchemaOptimizer(conn_params={
        'host': pg.host,
        'port': pg.port,
        'database': pg.database,
        'user': pg.user,
        'password': pg.password
    })

    plan = optimizer.analyze_table(schema, table)

    # Extraire infos pour métadonnées
    types_to_change = sum(1 for col in plan.columns if col.current_type != col.inferred_type)

    metadata = {
        "table": MetadataValue.text(f"{schema}.{table}"),
        "total_columns": MetadataValue.int(len(plan.columns)),
        "types_to_change": MetadataValue.int(types_to_change),
        "primary_keys": MetadataValue.json(plan.primary_keys),
        "foreign_keys": MetadataValue.json(plan.foreign_keys),
        "indexes_planned": MetadataValue.int(len(plan.indexes_to_create)),
        "has_geometry": MetadataValue.bool(plan.has_geometry),
    }

    context.log.info(f"✅ Analyse terminée:")
    context.log.info(f"   - {len(plan.columns)} colonnes")
    context.log.info(f"   - {types_to_change} types à optimiser")
    context.log.info(f"   - {len(plan.primary_keys)} PK détectées")
    context.log.info(f"   - {len(plan.foreign_keys)} FK détectées")
    context.log.info(f"   - {len(plan.indexes_to_create)} index à créer")

    return Output(
        value={
            "table": table,
            "columns": [
                {
                    "name": col.column_name,
                    "current_type": col.current_type,
                    "inferred_type": col.inferred_type,
                    "is_pk": col.is_primary_key,
                    "is_fk": col.is_foreign_key
                }
                for col in plan.columns
            ],
            "primary_keys": plan.primary_keys,
            "foreign_keys": plan.foreign_keys,
            "indexes": plan.indexes_to_create
        },
        metadata=metadata
    )


def create_schema_optimization_asset(resource_name: str):
    """
    Factory qui crée un asset d'optimisation de schéma pour une table donnée

    Cet asset s'exécute automatiquement APRÈS l'ingestion de la table CSV
    et optimise le schéma (TEXT → INTEGER/FLOAT/TIMESTAMP/etc.)

    Args:
        resource_name: Nom de la resource Hub'Eau (ex: piezometry_stations)
    """
    from dagster import AssetKey

    asset_name = f"{resource_name}_schema_optimized"
    upstream_asset = f"{resource_name}_csv"  # Dépend de l'asset d'ingestion

    @asset(
        name=asset_name,
        group_name="schema_management",
        description=f"Optimise automatiquement le schéma de {resource_name} après ingestion",
        deps=[AssetKey(upstream_asset)],  # Attend que l'ingestion soit terminée
        compute_kind="postgres",
        op_tags={"optimization": "schema", "table": resource_name}
    )
    def schema_optimization_asset(
        context: AssetExecutionContext,
        pg: PostgreSQLResource
    ) -> Output[Dict[str, Any]]:
        """
        Optimise le schéma de la table après ingestion:
        1. Analyse les données (inférence types, PK, FK)
        2. Applique les optimisations (ALTER, CREATE INDEX, etc.)
        3. Log les résultats
        """
        schema = "hubeau"
        table = resource_name

        context.log.info(f"🔧 Optimisation automatique du schéma: {schema}.{table}")

        # Créer optimizer
        optimizer = SchemaOptimizer(conn_params={
            'host': pg.host,
            'port': pg.port,
            'database': pg.database,
            'user': pg.user,
            'password': pg.password
        })

        # Analyser table
        context.log.info(f"📊 Analyse de {table}...")
        plan = optimizer.analyze_table(schema, table)

        # Log plan
        types_to_change = sum(1 for col in plan.columns if col.current_type != col.inferred_type)
        context.log.info(f"  - {len(plan.columns)} colonnes")
        context.log.info(f"  - {types_to_change} types à optimiser")
        context.log.info(f"  - {len(plan.primary_keys)} PK détectées: {plan.primary_keys}")
        context.log.info(f"  - {len(plan.foreign_keys)} FK détectées")
        context.log.info(f"  - {len(plan.indexes_to_create)} index à créer")

        # Appliquer optimisations (pas dry_run)
        context.log.info(f"🚀 Application des optimisations...")
        stats = optimizer.optimize_table(plan, dry_run=False)

        # Log résultats
        context.log.info(f"✅ Optimisation terminée:")
        context.log.info(f"  - {stats['types_changed']} types modifiés")
        context.log.info(f"  - {stats['indexes_created']} index créés")
        context.log.info(f"  - {'✅' if stats['pk_created'] else '❌'} PK créée")
        context.log.info(f"  - {stats['fks_created']} FK créées")

        if stats['errors']:
            context.log.warning(f"⚠️ {len(stats['errors'])} erreurs:")
            for error in stats['errors'][:5]:  # Log max 5 erreurs
                context.log.warning(f"  - {error}")

        return Output(
            value={
                "table": table,
                "types_changed": stats['types_changed'],
                "indexes_created": stats['indexes_created'],
                "pk_created": stats['pk_created'],
                "fks_created": stats['fks_created'],
                "errors": stats['errors']
            },
            metadata={
                "table": MetadataValue.text(f"{schema}.{table}"),
                "types_changed": MetadataValue.int(stats['types_changed']),
                "indexes_created": MetadataValue.int(stats['indexes_created']),
                "pk_created": MetadataValue.bool(stats['pk_created']),
                "fks_created": MetadataValue.int(stats['fks_created']),
                "errors_count": MetadataValue.int(len(stats['errors'])),
            }
        )

    return schema_optimization_asset


# ========================================
# GÉNÉRATION DES ASSETS D'OPTIMISATION
# ========================================

# Liste de TOUTES les tables Hub'Eau (référentiels + chroniques)
ALL_HUBEAU_TABLES = [
    # Chroniques/analyses (avec date filter)
    "piezometry_chroniques",
    "quality_groundwater_analyses",
    "quality_rivers_analyses",
    "quality_rivers_conditions",
    "quality_rivers_operations",
    "temperature_chroniques",
    "hydrometry_obs_elab",
    "hydrobio_indices",
    "hydrobio_taxons",
    "ecoulement_observations",
    "prelevements_chroniques",

    # Référentiels (stations, etc.)
    "piezometry_stations",
    "quality_groundwater_stations",
    "quality_rivers_stations",
    "temperature_stations",
    "hydrometry_sites",
    "hydrometry_stations",
    "hydrobio_stations",
    "ecoulement_stations",
    "ecoulement_campagnes",
    "prelevements_points",
    "prelevements_ouvrages",
]

# Créer un asset d'optimisation pour CHAQUE table
for table in ALL_HUBEAU_TABLES:
    globals()[f"{table}_schema_optimized"] = create_schema_optimization_asset(table)
