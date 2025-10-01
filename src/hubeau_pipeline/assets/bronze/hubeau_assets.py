"""
Assets Dagster pour l'ingestion Hub'Eau Bronze
Assets clairs et logiques pour chaque API Hub'Eau avec vraies APIs
"""

from dagster import (
    AssetExecutionContext, 
    AssetIn, 
    AssetKey, 
    DailyPartitionsDefinition, 
    StaticPartitionsDefinition,
    asset
)
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
# ✅ OPTIMISATION: Asset NON partitionné - récupère toujours les 30 derniers jours automatiquement

# Note: Toutes les APIs de campagnes sont maintenant en partitions ANNUELLES
# pour simplicité et cohérence (ONDE, Qualité Eau, Hydrobiologie)

# 🧪 PARTITIONS ANNUELLES : Pour APIs avec campagnes/prélèvements
# - ONDE : Campagnes mensuelles estivales (mai-octobre) → récupérées sur l'année
# - Qualité Cours d'Eau : Prélèvements mensuels/trimestriels → récupérés sur l'année
# - Qualité Nappes : Prélèvements semestriels/annuels → récupérés sur l'année
# - Hydrobiologie : Campagnes saisonnières (1-2x/an)
# - Prélèvements : Déclarations annuelles de volumes
# ✅ OPTIMISATION: 1 partition/an récupère toutes les campagnes/analyses de l'année avec leurs dates
YEARLY_PARTITIONS = StaticPartitionsDefinition(
    ["2020", "2021", "2022", "2023", "2024", "2025"]
)

# ====================================
# HELPER FUNCTIONS
# ====================================

async def ingest_hubeau_api(context: AssetExecutionContext, api_name: str) -> Dict[str, Any]:
    """Helper function pour l'ingestion d'une API Hub'Eau"""
    partition_key = context.partition_key
    
    # Gérer les différents formats de partitions :
    # - Annuelles (prélèvements) : "2024" → utiliser 01-01
    # - Mensuelles (ONDE) : "2024-08" → utiliser 01 du mois
    # - Quotidiennes (autres) : "2024-09-29" → utiliser tel quel
    if len(partition_key) == 4:  # Année seulement (ex: "2024")
        day = f"{partition_key}-01-01"  # Utiliser 1er janvier comme date de référence
    elif len(partition_key) == 7:  # Mois seulement (ex: "2024-08")
        day = f"{partition_key}-01"  # Utiliser 1er jour du mois
    else:
        day = partition_key

    # ✅ Éviter les requêtes Hub'Eau sur des partitions futures
    try:
        if len(partition_key) == 4:  # Année (ex: "2024")
            partition_dt = datetime(int(partition_key), 1, 1)
            is_future_partition = partition_dt.year > datetime.now().year
        elif len(partition_key) == 7:  # Mois (ex: "2024-08")
            partition_dt = datetime.fromisoformat(day)  # day = "2024-08-01"
            # Pour les mois, on vérifie si le 1er du mois est dans le futur
            is_future_partition = partition_dt.date() > datetime.now().date()
        else:  # Jour (ex: "2024-09-30")
            partition_dt = datetime.fromisoformat(day)
            is_future_partition = partition_dt.date() > datetime.now().date()
    except ValueError:
        partition_dt = None
        is_future_partition = False

    if is_future_partition:
        context.log.warning(
            "⏭️ Partition future détectée (%s) – ingestion Hub'Eau %s ignorée pour éviter erreurs 500 / données vides",
            partition_key,
            api_name,
        )
        return {
            "execution_date": datetime.now().isoformat(),
            "partition_date": partition_key,
            "api_name": api_name,
            "status": "skipped_future_partition",
            "total_records_ingested": 0,
            "message": f"Partition {partition_key} est dans le futur, pas de données disponibles"
        }

    context.log.info(f"🚀 Début ingestion {api_name} Hub'Eau pour {partition_key}")

    try:
        # Récupération de la configuration
        configs = get_all_hubeau_configs()
        config = configs[api_name]

        # Service d'ingestion réel
        minio_resource = getattr(context.resources, "s3", None)
        service = HubeauIngestionService(minio_resource=minio_resource)
        result = await service.ingest_api_data(config, day, partition_key=partition_key)
        
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
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    tags={"api": "hubeau"},
    description="🌊 Ingestion Hydrométrie Hub'Eau (débits et niveaux - ⚠️ 30 derniers jours automatique)"
)
async def hubeau_hydrometry_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Ingestion hydrométrie Hub'Eau - API v2 limitée aux 30 derniers jours
    ✅ OPTIMISATION: Asset NON partitionné - récupère automatiquement les 30 derniers jours
    """
    # Calculer la date d'il y a 30 jours
    date_30_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    context.log.info(f"🚀 Hydrométrie: Récupération automatique des 30 derniers jours (depuis {date_30_days_ago})")
    
    try:
        configs = get_all_hubeau_configs()
        config = configs["hydrometry"]
        
        minio_resource = getattr(context.resources, "s3", None)
        service = HubeauIngestionService(minio_resource=minio_resource)
        result = await service.ingest_api_data(config, date_30_days_ago, partition_key="recent_30days")
        
        context.log.info(f"✅ Ingestion hydrometry terminée: {result['total_records_ingested']} records")
        return result
        
    except Exception as e:
        context.log.error(f"❌ Erreur ingestion hydrometry: {str(e)}")
        return {
            "execution_date": datetime.now().isoformat(),
            "partition_date": "recent_30days",
            "api_name": "hydrometry",
            "status": "error",
            "error": str(e),
            "total_records_ingested": 0
        }

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
    partitions_def=YEARLY_PARTITIONS,  # ✅ OPTIMISATION: Partitions annuelles (récupère tous les prélèvements de l'année)
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    tags={"api": "hubeau"},  # ✅ Tag pour limitation de concurrence
    description="🌊 Ingestion Qualité Cours d'Eau Hub'Eau (analyses physico-chimiques - récupération annuelle)"
)
async def hubeau_water_quality_surface_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion qualité cours d'eau Hub'Eau - Partitions annuelles (récupère tous les prélèvements/analyses de l'année avec leurs dates)"""
    return await ingest_hubeau_api(context, "superficial_waterbodies_quality")

@asset(
    partitions_def=YEARLY_PARTITIONS,  # ✅ OPTIMISATION: Partitions annuelles (récupère tous les prélèvements de l'année)
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    tags={"api": "hubeau"},  # ✅ Tag pour limitation de concurrence
    description="🌊 Ingestion Qualité Nappes Hub'Eau (analyses physico-chimiques - récupération annuelle)"
)
async def hubeau_water_quality_groundwater_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion qualité nappes Hub'Eau - Partitions annuelles (récupère tous les prélèvements/analyses de l'année avec leurs dates)"""
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
    partitions_def=YEARLY_PARTITIONS,  # ✅ OPTIMISATION: Partitions annuelles (récupère toutes les campagnes de l'année)
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    tags={"api": "hubeau"},
    description="🌊 Ingestion ONDE Hub'Eau (Opération Nationale Des Étiages - récupération annuelle)"
)
async def hubeau_onde_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion ONDE Hub'Eau - Partitions annuelles (récupère toutes les campagnes estivales de l'année)"""
    return await ingest_hubeau_api(context, "onde")

@asset(
    partitions_def=YEARLY_PARTITIONS,  # ✅ OPTIMISATION: Partitions annuelles (campagnes saisonnières 1-2x/an)
    group_name="bronze_hubeau",
    required_resource_keys={"s3"},
    tags={"api": "hubeau"},  # ✅ Tag pour limitation de concurrence
    description="🐟 Ingestion Hydrobiologie Hub'Eau (indices biologiques - campagnes saisonnières annuelles)"
)
async def hubeau_hydrobiology_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion hydrobiologie Hub'Eau - Partitions annuelles adaptées aux campagnes saisonnières"""
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
    description="📊 Synthèse de l'ingestion Hub'Eau (2 APIs quotidiennes)",
    ins={
        "piezometry": AssetIn(key=AssetKey("hubeau_piezometry_bronze")),
        "temperature": AssetIn(key=AssetKey("hubeau_temperature_bronze")),
        # Note: APIs exclues (partitions incompatibles) :
        # - hubeau_hydrometry_bronze (non partitionné - 30 derniers jours automatique)
        # - hubeau_water_quality_surface_bronze (partitions annuelles)
        # - hubeau_water_quality_groundwater_bronze (partitions annuelles)
        # - hubeau_hydrobiology_bronze (partitions annuelles)
        # - hubeau_onde_bronze (partitions mensuelles)
        # - hubeau_prelevements_bronze (partitions annuelles)
    },
)
def hubeau_ingestion_summary(
    context: AssetExecutionContext,
    piezometry: Dict[str, Any],
    temperature: Dict[str, Any],
) -> Dict[str, Any]:
    """Synthétise les résultats d'ingestion des 2 APIs Hub'Eau à partitions quotidiennes (séries continues)."""

    day = context.partition_key
    context.log.info(f"📊 Génération du résumé d'ingestion Hub'Eau pour {day}")

    api_results = {
        "piezometry": piezometry,
        "temperature": temperature,
        # Note: hydrometry exclu (non partitionné)
        # Note: APIs à partitions mensuelles ou annuelles exclues
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
