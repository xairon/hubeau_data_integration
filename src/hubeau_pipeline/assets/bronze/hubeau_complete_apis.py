"""
Assets Bronze Hub'Eau - APIs Complètes (8 APIs)
4 APIs manquantes ajoutées avec gestion "1 observation/jour"
"""

from dagster import asset, DailyPartitionsDefinition, AssetExecutionContext, get_dagster_logger, RetryPolicy
from typing import Dict, Any
from .hubeau_real_ingestion import (
    DeduplicationConfig,
    EndpointConfig,
    HubeauAPIConfig,
    HubeauIngestionService,
)

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
        base_url="https://hubeau.eaufrance.fr/api/v2/qualite_rivieres",
        endpoints={
            "station_pc": EndpointConfig(
                path="station_pc",
                apply_temporal_filter=False,
                page_size=500,
            ),
            "analyse_pc": EndpointConfig(
                path="analyse_pc",
                temporal_param_keys=("date_debut_prelevement", "date_fin_prelevement"),
                lookback_days=365,
                page_size=500,
                deduplication=DeduplicationConfig(
                    date_field="date_prelevement",
                    group_keys=["code_station", "code_parametre"],
                ),
            ),
        },
        base_params={
            "format": "json",
        },
        timeout=180,
        default_lookback_days=365,
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
        endpoints={
            "stations": EndpointConfig(
                path="stations",
                apply_temporal_filter=False,
                page_size=1000,
            ),
            "campagnes": EndpointConfig(
                path="campagnes",
                temporal_param_keys=("date_debut_campagne", "date_fin_campagne"),
                lookback_days=365,
                page_size=1000,
            ),
            "observations": EndpointConfig(
                path="observations",
                temporal_param_keys=("date_debut_obs", "date_fin_obs"),
                lookback_days=120,
                page_size=1000,
                deduplication=DeduplicationConfig(
                    date_field="date_obs",
                    group_keys=["code_station"],
                ),
            ),
        },
        base_params={
            "format": "json",
        },
        timeout=180,
        default_lookback_days=365,
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
        endpoints={
            "stations": EndpointConfig(
                path="stations",
                apply_temporal_filter=False,
                page_size=500,
            ),
            "indices": EndpointConfig(
                path="indices",
                temporal_param_keys=("date_debut_operation", "date_fin_operation"),
                lookback_days=365,
                page_size=500,
                deduplication=DeduplicationConfig(
                    date_field="date_operation",
                    group_keys=["code_station", "code_indice"],
                ),
            ),
            "operationPrelevement": EndpointConfig(
                path="operationPrelevement",
                temporal_param_keys=("date_debut_operation", "date_fin_operation"),
                lookback_days=365,
                page_size=500,
            ),
        },
        base_params={
            "format": "json",
        },
        timeout=180,
        default_lookback_days=365,
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
        endpoints={
            "points_prelevement": EndpointConfig(
                path="points_prelevement",
                apply_temporal_filter=False,
                page_size=2000,
            ),
            "chroniques": EndpointConfig(
                path="chroniques",
                temporal_param_keys=("date_debut", "date_fin"),
                lookback_days=365,
                page_size=2000,
                deduplication=DeduplicationConfig(
                    date_field="date_debut_periode",
                    group_keys=["code_point_prelevement"],
                    truncate_to_day=False,
                ),
            ),
        },
        base_params={
            "format": "json",
        },
        timeout=180,
        default_lookback_days=365,
    )
    
    service = HubeauIngestionService()
    return service.ingest_hubeau_api(config, day)
