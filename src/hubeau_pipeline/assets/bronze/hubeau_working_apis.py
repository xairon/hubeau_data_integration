"""
Assets Bronze Hub'Eau - APIs FONCTIONNELLES SEULEMENT
Basé sur les tests réels des endpoints Hub'Eau
"""

from dagster import asset, DailyPartitionsDefinition, AssetExecutionContext, get_dagster_logger, RetryPolicy
from typing import Dict, Any
from .hubeau_real_ingestion import (
    EndpointConfig,
    HubeauAPIConfig,
    HubeauIngestionService,
)

# Partitions quotidiennes
DAILY_PARTITIONS = DailyPartitionsDefinition(start_date="2024-09-01")

# ====================================
# ASSET ONDE - FONCTIONNEL
# ====================================

@asset(
    group_name="bronze_hubeau",
    partitions_def=DAILY_PARTITIONS, 
    description="🌊 Hub'Eau ONDE (Écoulements) - API FONCTIONNELLE",
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def hubeau_onde_bronze_real(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Ingestion RÉELLE API ONDE (Observatoire National Des Étiages)
    - URL: /api/v1/ecoulement/ ✅ FONCTIONNELLE
    - Endpoints: campagnes ✅ (stations retourne 206 mais fonctionne)
    - Fréquence: Campagnes saisonnières (été principalement)
    """
    logger = get_dagster_logger()
    day = context.partition_key
    logger.info(f"🌊 Démarrage ingestion ONDE FONCTIONNELLE {day}")
    
    config = HubeauAPIConfig(
        name="onde",
        base_url="https://hubeau.eaufrance.fr/api/v1/ecoulement",
        endpoints={
            "campagnes": EndpointConfig(
                path="campagnes",
                temporal_param_keys=("date_debut_campagne", "date_fin_campagne"),
                page_size=1000,
                lookback_days=365,
            ),
        },
        base_params={
            "format": "json",
        },
    )
    
    service = HubeauIngestionService()
    return service.ingest_hubeau_api(config, day)

# ====================================
# ASSETS NON DISPONIBLES - DÉSACTIVÉS
# ====================================

@asset(
    group_name="bronze_hubeau_disabled",
    partitions_def=DAILY_PARTITIONS,
    description="❌ APIs Hub'Eau NON DISPONIBLES - Tous endpoints retournent 404",
    retry_policy=RetryPolicy(max_retries=1, delay=60)
)
def hubeau_apis_unavailable_bronze_real(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    APIs Hub'Eau NON DISPONIBLES
    - Qualité Surface: Tous endpoints retournent 404
    - Hydrobiologie: Tous endpoints retournent 404  
    - Prélèvements: Tous endpoints retournent 404/500
    - Qualité Eaux Souterraines: Endpoint stations retourne 404
    """
    logger = get_dagster_logger()
    day = context.partition_key
    logger.warning(f"⚠️ APIs Hub'Eau non disponibles pour {day}")
    
    return {
        "api_name": "apis_unavailable",
        "status": "apis_not_available", 
        "message": "Plusieurs APIs Hub'Eau non disponibles - endpoints retournent 404",
        "day": day,
        "total_records": 0,
        "unavailable_apis": [
            "qualite_surface",
            "hydrobiologie", 
            "prelevements",
            "qualite_eaux_souterraines_stations"
        ],
        "available_apis": [
            "piezo",
            "temperature", 
            "hydro_observations",
            "onde_campagnes"
        ]
    }
