"""
Assets Bronze - Ingestion RÉELLE avec connexions APIs
Hub'Eau + BDLISA + Sandre avec gestion d'erreurs professionnelle
"""

# Hub'Eau APIs - Implémentation réelle (4 APIs principales existantes)
from .hubeau_real_ingestion import (
    hubeau_piezo_bronze_real,
    hubeau_hydro_bronze_real,
    hubeau_quality_groundwater_bronze_real,
    hubeau_temperature_bronze_real,
    hubeau_quality_surface_bronze_real,
    hubeau_onde_bronze_real,
    hubeau_hydrobiologie_bronze_real,
    hubeau_prelevements_bronze_real
)

# Sources externes - Implémentation réelle
from .bdlisa_real_ingestion import bdlisa_geographic_bronze_real
from .sandre_real_ingestion import sandre_thesaurus_bronze_real

# Assets de production RÉELS - TOUTES LES 8 APIs HUB'EAU
production_bronze_assets = [
    # Hub'Eau 4 APIs principales existantes
    hubeau_piezo_bronze_real,
    hubeau_hydro_bronze_real,
    hubeau_quality_groundwater_bronze_real,
    hubeau_temperature_bronze_real,
    
    # Hub'Eau 4 nouvelles APIs refactorisées
    hubeau_quality_surface_bronze_real,
    hubeau_onde_bronze_real,
    hubeau_hydrobiologie_bronze_real,
    hubeau_prelevements_bronze_real,
    
    # Sources externes
    bdlisa_geographic_bronze_real,
    sandre_thesaurus_bronze_real
]

# Hub'Eau APIs complémentaires (à implémenter si besoin)
# from .hubeau_complementary import (
#     hubeau_ecoulement_bronze,
#     hubeau_hydrobiologie_bronze,
#     hubeau_prelevements_bronze,
# )

__all__ = [
    # Assets de production réels
    "production_bronze_assets",
    
    # Hub'Eau 8 APIs complètes
    "hubeau_piezo_bronze_real",
    "hubeau_hydro_bronze_real", 
    "hubeau_quality_groundwater_bronze_real",
    "hubeau_temperature_bronze_real",
    "hubeau_quality_surface_bronze_real",
    "hubeau_onde_bronze_real",
    "hubeau_hydrobiologie_bronze_real",
    "hubeau_prelevements_bronze_real",
    
    # Externes
    "bdlisa_geographic_bronze_real",
    "sandre_thesaurus_bronze_real"
]