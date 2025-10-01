"""
Assets Dagster pour l'ingestion Hub'Eau Bronze
Assets clairs et logiques pour chaque API Hub'Eau avec vraies APIs
"""

from dagster import AssetExecutionContext, AssetIn, AssetKey, DailyPartitionsDefinition, asset
from datetime import datetime, timedelta
from typing import Dict, Any
import asyncio

# Import du vrai service d'ingestion et des configurations
from .hubeau_client import HubeauIngestionService
from .hubeau_configs import get_all_hubeau_configs

# Configuration des partitions journalières
# Compromis : 3 ans d'historique (~1100 partitions)
# Note: Température/Hydrobiologie/Piézométrie ont données historiques
DAILY_PARTITIONS = DailyPartitionsDefinition(start_date="2022-01-01")

# ⚠️ HYDROMÉTRIE : API v2 limitée aux 30 derniers jours SEULEMENT
# Restriction Hub'Eau : "date can't be < 1 month from now"
from dagster import StaticPartitionsDefinition

HYDROMETRY_RECENT_PARTITIONS = DailyPartitionsDefinition(
    start_date=(datetime.now() - timedelta(days=29)).strftime("%Y-%m-%d"),
    end_offset=0  # Jusqu'à aujourd'hui
)

# 💧 PRÉLÈVEMENTS : Partitions annuelles (déclarations de volumes annuels)
# Note: Dagster n'a pas YearlyPartitionsDefinition, on utilise StaticPartitionsDefinition
YEARLY_PARTITIONS = StaticPartitionsDefinition(
    ["2020", "2021", "2022", "2023", "2024", "2025"]
)

# ====================================
# HELPER FUNCTIONS
# ====================================

async def ingest_hubeau_api(context: AssetExecutionContext, api_name: str) -> Dict[str, Any]:
    """Helper function pour l'ingestion d'une API Hub'Eau"""
    partition_key = context.partition_key
    
    # Pour prélèvements : partition_key = "2024" (année)
    # Pour autres APIs : partition_key = "2024-09-29" (date)
    if len(partition_key) == 4:  # Année seulement (ex: "2024")
        day = f"{partition_key}-01-01"  # Utiliser 1er janvier comme date de référence
    else:
        day = partition_key

    # Éviter les requêtes Hub'Eau sur des partitions futures (observé sur ONDE → erreurs 500)
    try:
        if len(partition_key) == 4:
            partition_dt = datetime(int(partition_key), 1, 1)
            is_future_partition = partition_dt.year > datetime.now().year
        else:
            partition_dt = datetime.fromisoformat(day)
            is_future_partition = partition_dt.date() > datetime.now().date()
    except ValueError:
        partition_dt = None
        is_future_partition = False

    if is_future_partition:
        context.log.warning(
            "⏭️ Partition future détectée (%s) – ingestion Hub'Eau %s ignorée pour éviter les erreurs 500",
            partition_key,
            api_name,
        )
        return {
            "execution_date": datetime.now().isoformat(),
            "partition_date": partition_key,
            "api_name": api_name,
            "status": "skipped_future_partition",
            "total_records_ingested": 0,
        }

    context.log.info(f"🚀 Début ingestion {api_name} Hub'Eau pour {partition_key}")

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
            "partition_date": partition_key,  # Utiliser partition_key au lieu de day
            "api_name": api_name,
            "status": "error",
            "error": str(e),
            "total_records_ingested": 0
        }

# ====================================
# ASSETS HUB'EAU BRONZE
# ====================================

@asset(
    partitions_def=HYDROMETRY_RECENT_PARTITIONS,  # ⚠️ 30 jours max (restriction API v2)
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    tags={"api": "hubeau"},  # ✅ Tag pour limitation de concurrence
    description="🌊 Ingestion Hydrométrie Hub'Eau (débits et niveaux - ⚠️ 30 derniers jours uniquement)"
)
async def hubeau_hydrometry_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion hydrométrie Hub'Eau - API v2 limitée aux 30 derniers jours"""
    return await ingest_hubeau_api(context, "hydrometry")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    tags={"api": "hubeau"},  # ✅ Tag pour limitation de concurrence
    description="🏔️ Ingestion Piézométrie Hub'Eau (niveaux des nappes phréatiques)"
)
async def hubeau_piezometry_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion piézométrie Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "piezometry")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    tags={"api": "hubeau"},  # ✅ Tag pour limitation de concurrence
    description="🌊 Ingestion Qualité Cours d'Eau Hub'Eau (analyses physico-chimiques)"
)
async def hubeau_water_quality_surface_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion qualité cours d'eau Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "superficial_waterbodies_quality")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    tags={"api": "hubeau"},  # ✅ Tag pour limitation de concurrence
    description="🌊 Ingestion Qualité Nappes Hub'Eau (analyses physico-chimiques)"
)
async def hubeau_water_quality_groundwater_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion qualité nappes Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "ground_water_quality")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    tags={"api": "hubeau"},  # ✅ Tag pour limitation de concurrence
    description="🌡️ Ingestion Température Hub'Eau (température des cours d'eau)"
)
async def hubeau_temperature_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion température Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "temperature")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    tags={"api": "hubeau"},  # ✅ Tag pour limitation de concurrence
    description="🌊 Ingestion ONDE Hub'Eau (Opération Nationale Des Étiages)"
)
async def hubeau_onde_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion ONDE Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "onde")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    tags={"api": "hubeau"},  # ✅ Tag pour limitation de concurrence
    description="🐟 Ingestion Hydrobiologie Hub'Eau (indices biologiques)"
)
async def hubeau_hydrobiology_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion hydrobiologie Hub'Eau avec vraies APIs"""
    return await ingest_hubeau_api(context, "hydrobiology")

@asset(
    partitions_def=YEARLY_PARTITIONS,  # 💧 Partitions ANNUELLES (données de volumes annuels)
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    tags={"api": "hubeau"},  # ✅ Tag pour limitation de concurrence
    description="💧 Ingestion Prélèvements Hub'Eau (volumes annuels de prélèvements)"
)
async def hubeau_prelevements_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion prélèvements Hub'Eau - Données annuelles de volumes prélevés"""
    return await ingest_hubeau_api(context, "prelevements")

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    description="📊 Synthèse de l'ingestion Hub'Eau (7 APIs quotidiennes)",
    ins={
        "hydrometry": AssetIn(key=AssetKey("hubeau_hydrometry_bronze")),
        "piezometry": AssetIn(key=AssetKey("hubeau_piezometry_bronze")),
        "surface_quality": AssetIn(key=AssetKey("hubeau_water_quality_surface_bronze")),
        "groundwater_quality": AssetIn(key=AssetKey("hubeau_water_quality_groundwater_bronze")),
        "temperature": AssetIn(key=AssetKey("hubeau_temperature_bronze")),
        "onde": AssetIn(key=AssetKey("hubeau_onde_bronze")),
        "hydrobiology": AssetIn(key=AssetKey("hubeau_hydrobiology_bronze")),
        # Note: hubeau_prelevements_bronze exclu (partitions annuelles incompatibles)
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
) -> Dict[str, Any]:
    """Synthétise les résultats d'ingestion des 7 APIs Hub'Eau à partitions quotidiennes."""

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
        # Note: prelevements exclu (partitions annuelles - a son propre job)
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
