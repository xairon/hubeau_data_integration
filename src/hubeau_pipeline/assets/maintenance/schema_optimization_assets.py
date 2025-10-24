"""
Assets de maintenance pour l'optimisation de schéma

À lancer APRÈS le chargement initial des données CSV
"""

import os
from dagster import asset, AssetExecutionContext
from hubeau_pipeline.utils.schema_optimizer import optimize_all_csv_tables


@asset(
    group_name="maintenance",
    description="Optimise les types de colonnes des tables CSV après ingestion complète"
)
def optimize_csv_schemas(context: AssetExecutionContext) -> dict:
    """
    Optimise les types de colonnes pour toutes les tables CSV

    Stratégie:
    1. Analyse chaque colonne TEXT
    2. Détecte si peut être INTEGER, NUMERIC, TIMESTAMP, VARCHAR
    3. Convertit vers le type optimal

    Bénéfices:
    - Queries plus rapides (index sur types natifs)
    - Stockage réduit (INTEGER vs TEXT)
    - Meilleure intégrité des données
    """

    conn_params = {
        "host": os.getenv("PG_HOST", "postgres"),
        "port": os.getenv("PG_PORT", "5432"),
        "database": os.getenv("PG_DB", "postgres"),
        "user": os.getenv("PG_USER", "postgres"),
        "password": os.getenv("PG_PASSWORD"),
    }

    context.log.info("🔧 Démarrage de l'optimisation des schémas CSV...")

    results = optimize_all_csv_tables(conn_params, schema_name="hubeau")

    # Résumé pour Dagster
    total_opts = sum(len(opts) for opts in results.values())
    context.log.info(f"✅ {total_opts} colonnes optimisées sur {len(results)} tables")

    # Retourner les stats
    return {
        "total_tables": len(results),
        "total_optimizations": total_opts,
        "details": {
            table: len(opts) for table, opts in results.items()
        }
    }
