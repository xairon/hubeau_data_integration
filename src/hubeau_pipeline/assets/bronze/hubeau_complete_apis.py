"""
Assets Bronze Hub'Eau - APIs Complètes (8 APIs)
4 APIs manquantes ajoutées avec gestion "1 observation/jour"
"""

from dagster import asset, DailyPartitionsDefinition, AssetExecutionContext, get_dagster_logger, RetryPolicy
from datetime import datetime, timedelta
from typing import Dict, Any
from .hubeau_real_ingestion import HubeauAPIConfig, HubeauIngestionService

# Configuration des partitions journalières  
# HYDRO: Limitation 1 mois historique → démarrage récent
DAILY_PARTITIONS = DailyPartitionsDefinition(start_date="2024-09-01")

# ====================================
# ASSET QUALITÉ COURS D'EAU (URL CORRIGÉE)
# ====================================

@asset(
    group_name="bronze_hubeau", 
    partitions_def=DAILY_PARTITIONS,
    description="🧪 Hub'Eau Qualité Cours d'Eau - URL corrigée + sampling quotidien",
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def hubeau_quality_surface_bronze_real(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Ingestion RÉELLE API qualité des cours d'eau Hub'Eau
    - URL CORRIGÉE: /api/v1/qualite_rivieres (pas qualite_cours_eau!)
    - Endpoints: station_pc, analyse_pc  
    - Sampling: 1 analyse par jour par station maximum
    """
    logger = get_dagster_logger()
    day = context.partition_key
    logger.info(f"🧪 Démarrage ingestion qualité cours d'eau COMPLÈTE {day}")
    
    # Configuration API qualité surface selon documentation officielle
    config = HubeauAPIConfig(
        name="quality_surface",
        base_url="https://hubeau.eaufrance.fr/api/v1/qualite_eau_surface",  # URL corrigée selon tests
        endpoints=["stations", "analyses"],  # Endpoints corrigés selon tests
        params={
            "format": "json",
            "size": 5000  # Limite adaptée pour analyses qualité
        }
    )
    
    service = HubeauIngestionService()
    return service.ingest_hubeau_api(config, day)

# ====================================  
# ASSET ÉCOULEMENT ONDE
# ====================================

@asset(
    group_name="bronze_hubeau",
    partitions_def=DAILY_PARTITIONS, 
    description="🌊 Hub'Eau ONDE (Écoulements) - Ingestion RÉELLE",
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def hubeau_onde_bronze_real(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Ingestion RÉELLE API ONDE (Observatoire National Des Étiages)
    - URL: /api/v1/ecoulement/
    - Endpoints: stations, campagnes, observations
    - Fréquence: Campagnes saisonnières (été principalement)
    """
    logger = get_dagster_logger()
    day = context.partition_key
    logger.info(f"🌊 Démarrage ingestion ONDE COMPLÈTE {day}")
    
    config = HubeauAPIConfig(
        name="onde",
        base_url="https://hubeau.eaufrance.fr/api/v1/ecoulement",
        endpoints=["stations", "campagnes", "observations"],
        params={
            "format": "json",
            "size": 5000  # Limite adaptée pour campagnes ONDE
        }
    )
    
    service = HubeauIngestionService()
    return service.ingest_hubeau_api(config, day)

# ====================================
# ASSET HYDROBIOLOGIE  
# ====================================

@asset(
    group_name="bronze_hubeau",
    partitions_def=DAILY_PARTITIONS,
    description="🐟 Hub'Eau Hydrobiologie - Ingestion RÉELLE", 
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def hubeau_hydrobiologie_bronze_real(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Ingestion RÉELLE API Hydrobiologie Hub'Eau
    - URL: /api/v1/hydrobiologie/
    - Endpoints: stations, indices, operationPrelevement
    - Fréquence: Campagnes annuelles/saisonnières
    """
    logger = get_dagster_logger()
    day = context.partition_key
    logger.info(f"🐟 Démarrage ingestion Hydrobiologie COMPLÈTE {day}")
    
    config = HubeauAPIConfig(
        name="hydrobiologie",
        base_url="https://hubeau.eaufrance.fr/api/v1/hydrobiologie",
        endpoints=["stations", "indices", "operationPrelevement"],
        params={
            "format": "json",
            "size": 3000  # Limite adaptée pour données biologiques
        }
    )
    
    service = HubeauIngestionService()
    return service.ingest_hubeau_api(config, day)

# ====================================
# ASSET PRÉLÈVEMENTS
# ====================================

@asset(
    group_name="bronze_hubeau",
    partitions_def=DAILY_PARTITIONS,
    description="🚰 Hub'Eau Prélèvements - Ingestion RÉELLE",
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def hubeau_prelevements_bronze_real(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Ingestion RÉELLE API Prélèvements Hub'Eau
    - URL: /api/v1/prelevements/
    - Endpoints: points_prelevement, chroniques
    - Données: Volumes prélevés déclarés
    """
    logger = get_dagster_logger()
    day = context.partition_key
    logger.info(f"🚰 Démarrage ingestion Prélèvements COMPLÈTE {day}")
    
    config = HubeauAPIConfig(
        name="prelevements", 
        base_url="https://hubeau.eaufrance.fr/api/v1/prelevements",
        endpoints=["points_prelevement", "chroniques"],
        params={
            "format": "json",
            "size": 10000  # Limite adaptée pour prélèvements
        }
    )
    
    service = HubeauIngestionService()
    return service.ingest_hubeau_api(config, day)
