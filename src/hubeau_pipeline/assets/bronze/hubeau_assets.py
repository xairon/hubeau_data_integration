"""
Assets Dagster pour l'ingestion Hub'Eau Bronze
Assets clairs et logiques pour chaque API Hub'Eau avec vraies APIs
"""

from dagster import AssetExecutionContext, AssetIn, AssetKey, DailyPartitionsDefinition, asset
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
        minio_resource = getattr(context.resources, "s3", None)
        service = HubeauIngestionService(minio_resource=minio_resource)
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
    required_resource_keys={"s3"},
    description="🌊 Ingestion Hydrométrie Hub'Eau (débits et niveaux des cours d'eau)"
)
async def hubeau_hydrometry_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion hydrométrie Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "hydrometry")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    description="🏔️ Ingestion Piézométrie Hub'Eau (niveaux des nappes phréatiques)"
)
async def hubeau_piezometry_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion piézométrie Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "piezometry")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    description="🌊 Ingestion Qualité Cours d'Eau Hub'Eau (analyses physico-chimiques)"
)
async def hubeau_water_quality_surface_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion qualité cours d'eau Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "superficial_waterbodies_quality")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    description="🌊 Ingestion Qualité Nappes Hub'Eau (analyses physico-chimiques)"
)
async def hubeau_water_quality_groundwater_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion qualité nappes Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "ground_water_quality")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    description="🌡️ Ingestion Température Hub'Eau (température des cours d'eau)"
)
async def hubeau_temperature_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion température Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "temperature")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    description="🌊 Ingestion ONDE Hub'Eau (Opération Nationale Des Étiages)"
)
async def hubeau_onde_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion ONDE Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "onde")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    description="🐟 Ingestion Hydrobiologie Hub'Eau (indices biologiques)"
)
async def hubeau_hydrobiology_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion hydrobiologie Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "hydrobiology")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    description="💧 Ingestion Prélèvements Hub'Eau (prélèvements en eau)"
)
async def hubeau_prelevements_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion prélèvements Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "prelevements")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    description="📊 Synthèse de l'ingestion Hub'Eau",
    ins={
        "hydrometry": AssetIn(key=AssetKey("hubeau_hydrometry_bronze")),
        "piezometry": AssetIn(key=AssetKey("hubeau_piezometry_bronze")),
        "surface_quality": AssetIn(key=AssetKey("hubeau_water_quality_surface_bronze")),
        "groundwater_quality": AssetIn(key=AssetKey("hubeau_water_quality_groundwater_bronze")),
        "temperature": AssetIn(key=AssetKey("hubeau_temperature_bronze")),
        "onde": AssetIn(key=AssetKey("hubeau_onde_bronze")),
        "hydrobiology": AssetIn(key=AssetKey("hubeau_hydrobiology_bronze")),
        "prelevements": AssetIn(key=AssetKey("hubeau_prelevements_bronze")),
    },
)
def hubeau_ingestion_summary(
    context: AssetExecutionContext,
    hydrometry: Dict[str, Any],
    piezometry: Dict[str, Any],
    surface_quality: Dict[str, Any],
    groundwater_quality: Dict[str, Any],
    temperature: Dict[str, Any],
    onde: Dict[str, Any],
    hydrobiology: Dict[str, Any],
    prelevements: Dict[str, Any],
) -> Dict[str, Any]:
    """Synthétise les résultats d'ingestion de toutes les APIs Hub'Eau."""

    day = context.partition_key
    context.log.info(f"📊 Génération du résumé d'ingestion Hub'Eau pour {day}")

    api_results = {
        "hydrometry": hydrometry,
        "piezometry": piezometry,
        "superficial_waterbodies_quality": surface_quality,
        "ground_water_quality": groundwater_quality,
        "temperature": temperature,
        "onde": onde,
        "hydrobiology": hydrobiology,
        "prelevements": prelevements,
    }

    total_records = 0
    summary_by_api: Dict[str, Any] = {}
    statuses = []

    for api_name, payload in api_results.items():
        records = payload.get("total_records_ingested", 0)
        status = payload.get("status", "unknown")
        statuses.append(status)
        total_records += records

        endpoints_breakdown = {
            endpoint: details.get("records_count", 0)
            for endpoint, details in payload.get("results_by_endpoint", {}).items()
        }

        summary_by_api[api_name] = {
            "status": status,
            "records": records,
            "endpoints": endpoints_breakdown,
        }

    if any(status == "error" for status in statuses):
        overall_status = "error"
    elif statuses and all(status == "no_data" for status in statuses):
        overall_status = "no_data"
    else:
        overall_status = "success"

    context.add_output_metadata(
        {
            "total_records": total_records,
            "apis": summary_by_api,
        }
    )

    return {
        "execution_date": datetime.now().isoformat(),
        "partition_date": day,
        "api_name": "ingestion_summary",
        "status": overall_status,
        "total_records_ingested": total_records,
        "results_by_api": summary_by_api,
    }
