"""
Assets Dagster pour l'ingestion Hub'Eau Bronze
Assets clairs et logiques pour chaque API Hub'Eau avec vraies APIs
"""

from dagster import asset, DailyPartitionsDefinition, AssetExecutionContext, get_dagster_logger
from datetime import datetime
from typing import Dict, Any
import asyncio

# Import du vrai service d'ingestion et des configurations
from .hubeau_client import HubeauIngestionService
from .hubeau_configs import get_all_hubeau_configs

# Configuration des partitions journalières
DAILY_PARTITIONS = DailyPartitionsDefinition(start_date="2024-09-01")

# ====================================
# HELPER FUNCTIONS
# ====================================

async def ingest_hubeau_api(context: AssetExecutionContext, api_name: str) -> Dict[str, Any]:
    """Helper function pour l'ingestion d'une API Hub'Eau"""
    day = context.partition_key
    context.log.info(f"🚀 Début ingestion {api_name} Hub'Eau pour {day}")
    
    try:
        # Récupération de la configuration
        configs = get_all_hubeau_configs()
        config = configs[api_name]
        
        # Service d'ingestion réel
        service = HubeauIngestionService()
        result = await service.ingest_api_data(config, day)
        
        context.log.info(f"✅ Ingestion {api_name} terminée: {result['total_records_ingested']} records")
        return result
        
    except Exception as e:
        context.log.error(f"❌ Erreur ingestion {api_name}: {str(e)}")
        return {
            "execution_date": datetime.now().isoformat(),
            "partition_date": day,
            "api_name": api_name,
            "status": "error",
            "error": str(e),
            "total_records_ingested": 0
        }

# ====================================
# ASSETS HUB'EAU BRONZE
# ====================================

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    description="🌊 Ingestion Hydrométrie Hub'Eau (débits et niveaux des cours d'eau)"
)
async def hubeau_hydrometry_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion hydrométrie Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "hydrometry")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    description="🏔️ Ingestion Piézométrie Hub'Eau (niveaux des nappes phréatiques)"
)
async def hubeau_piezometry_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion piézométrie Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "piezometry")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    description="🌊 Ingestion Qualité Cours d'Eau Hub'Eau (analyses physico-chimiques)"
)
async def hubeau_water_quality_surface_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion qualité cours d'eau Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "superficial_waterbodies_quality")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    description="🌊 Ingestion Qualité Nappes Hub'Eau (analyses physico-chimiques)"
)
async def hubeau_water_quality_groundwater_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion qualité nappes Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "ground_water_quality")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    description="🌡️ Ingestion Température Hub'Eau (température des cours d'eau)"
)
async def hubeau_temperature_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion température Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "temperature")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    description="🌊 Ingestion ONDE Hub'Eau (Opération Nationale Des Étiages)"
)
async def hubeau_onde_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion ONDE Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "onde")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    description="🐟 Ingestion Hydrobiologie Hub'Eau (indices biologiques)"
)
async def hubeau_hydrobiology_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion hydrobiologie Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "hydrobiology")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    description="💧 Ingestion Prélèvements Hub'Eau (prélèvements en eau)"
)
async def hubeau_prelevements_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion prélèvements Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "prelevements")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    description="📊 Synthèse de l'ingestion Hub'Eau"
)
async def hubeau_ingestion_summary(context: AssetExecutionContext) -> Dict[str, Any]:
    """Synthèse de l'ingestion Hub'Eau"""
    day = context.partition_key
    context.log.info(f"📊 Génération du résumé d'ingestion Hub'Eau pour {day}")
    
    # Récupération des résultats de tous les assets Hub'Eau
    # (Cette logique sera implémentée plus tard avec les dépendances Dagster)
    return {
        "execution_date": datetime.now().isoformat(),
        "partition_date": day,
        "api_name": "ingestion_summary",
        "status": "success",
        "message": "Synthèse générée - dépendances à implémenter",
        "total_records_ingested": 0
    }