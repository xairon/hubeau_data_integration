"""
Assets Hub'Eau - Architecture Dagster professionnelle
Bronze → Silver → Gold avec optimisations
"""

# Import des assets par couche - VERSION RÉELLE (Bronze seulement)
from .bronze import production_bronze_assets

# Silver et Gold temporairement désactivés (imports cassés)
# from .silver import (
#     piezo_timescale_optimized, quality_timescale_optimized,
#     bdlisa_postgis_silver, sandre_neo4j_silver
# )
# from .gold import (
#     sosa_ontology_production, integrated_analytics_production,
#     demo_quality_scores, demo_neo4j_showcase
# )

# Assets de production (calculs réels)
production_assets = [
    # Bronze - Assets réels avec vraies connexions
    *production_bronze_assets,
    
    # Silver - À réimplémenter avec vraies transformations
    # piezo_timescale_optimized,
    # quality_timescale_optimized,
    # bdlisa_postgis_silver,
    # sandre_neo4j_silver,
    
    # Gold - Production future
    # sosa_ontology_production,
    # integrated_analytics_production
]

# Assets de démonstration (temporairement désactivés)
# demo_assets = [
#     demo_quality_scores,
#     demo_neo4j_showcase
# ]

# Tous les assets - BRONZE SEULEMENT pour l'instant
all_assets = production_assets  # Seulement Bronze réels pour tests
# all_assets = production_assets + demo_assets  # Complet quand Silver/Gold prêts

__all__ = [
    "all_assets",
    "production_assets"
    # "demo_assets"  # Temporairement désactivé
]