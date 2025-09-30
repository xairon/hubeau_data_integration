"""
Configurations Hub'Eau centralisées
Définition des paramètres pour chaque API Hub'Eau
"""

from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field
from .hubeau_client import HubeauApiConfig, HubeauEndpointConfig

# ====================================
# CONFIGURATIONS DES APIs HUB'EAU
# ====================================

def get_all_hubeau_configs() -> Dict[str, HubeauApiConfig]:
    """Retourne toutes les configurations Hub'Eau"""
    return {
        "hydrometry": get_hydrometry_config(),
        "piezometry": get_piezometry_config(),
        "superficial_waterbodies_quality": get_superficial_waterbodies_quality_config(),
        "ground_water_quality": get_ground_water_quality_config(),
        "temperature": get_temperature_config(),
        "onde": get_onde_config(),
        "hydrobiology": get_hydrobiology_config(),
        "prelevements": get_prelevements_config()
    }

def get_hydrometry_config() -> HubeauApiConfig:
    """Configuration API Hydrométrie"""
    return HubeauApiConfig(
        name="hydrometry",
        base_url="https://hubeau.eaufrance.fr/api/v2/hydrometrie",
        version="v2",
        endpoints={
                   "referentiel_sites": HubeauEndpointConfig(
                       path="referentiel/sites",
                       page_size=5000,
                       max_pages=10,
                       cache_duration=30,
                       requires_spatial_filter=True,
                       spatial_params={"dept": "code_departement"},
                       depth_limit=50000  # Limite élevée pour éviter troncature
                   ),
            "referentiel_stations": HubeauEndpointConfig(
                path="referentiel/stations",
                page_size=5000,
                max_pages=10,
                cache_duration=30,
                requires_spatial_filter=True,
                spatial_params={"dept": "code_departement"},
                depth_limit=50000  # Limite élevée pour éviter troncature
            ),
                   "observations_tr": HubeauEndpointConfig(
                       path="observations_tr",
                       temporal_params={"start": "date_debut_obs", "end": "date_fin_obs"},
                       page_size=1000,
                       max_pages=50,  # Limite raisonnable par station (50k records max)
                       supports_cursor=True,
                       realtime_cache_duration=15,
                       cache_duration=30,
                       # ✅ CORRECTIF: Désactiver filtre spatial, utiliser approche par code_entite
                       requires_spatial_filter=False,
                       depth_limit=None
                   ),
                   "obs_elab": HubeauEndpointConfig(
                       path="obs_elab",
                       temporal_params={"start": "date_debut_obs_elab", "end": "date_fin_obs_elab"},
                       page_size=1000,
                       max_pages=50,  # Limite raisonnable par station (50k records max)
                       cache_duration=30,
                       # ✅ CORRECTIF: Désactiver filtre spatial, utiliser approche par code_entite
                       requires_spatial_filter=False,
                       depth_limit=None
                   )
        }
    )

def get_piezometry_config() -> HubeauApiConfig:
    """Configuration API Piézométrie"""
    return HubeauApiConfig(
        name="piezometry",
        base_url="https://hubeau.eaufrance.fr/api/v1/niveaux_nappes",
        version="v1",
        endpoints={
                   "stations": HubeauEndpointConfig(
                       path="stations",
                       page_size=2000,
                       max_pages=10,
                       requires_spatial_filter=True,
                       spatial_params={"dept": "code_departement"},
                       cache_duration=30,
                       depth_limit=50000  # Limite élevée pour éviter troncature
                   ),
                   "chroniques_tr": HubeauEndpointConfig(
                       path="chroniques_tr",
                       temporal_params={"start": "date_debut_mesure", "end": "date_fin_mesure"},
                       page_size=1000,
                       max_pages=20,
                       requires_spatial_filter=False,  # ✅ CORRIGÉ: utiliser code_bss au lieu de départements
                       realtime_cache_duration=15,
                       cache_duration=30,
                       depth_limit=100000  # Limite très élevée pour chroniques
                   ),
                   "chroniques": HubeauEndpointConfig(
                       path="chroniques",
                       temporal_params={"start": "date_debut_mesure", "end": "date_fin_mesure"},
                       page_size=1000,
                       max_pages=20,
                       requires_spatial_filter=False,  # ✅ CORRIGÉ: utiliser code_bss au lieu de départements
                       cache_duration=30,
                       depth_limit=100000  # Limite très élevée pour chroniques
                   )
        }
    )

def get_superficial_waterbodies_quality_config() -> HubeauApiConfig:
    """Configuration API Qualité Cours d'Eau"""
    return HubeauApiConfig(
        name="superficial_waterbodies_quality",
        base_url="https://hubeau.eaufrance.fr/api/v2/qualite_rivieres",
        version="v2",
        endpoints={
                   "station_pc": HubeauEndpointConfig(
                       path="station_pc",
                       page_size=5000,
                       max_pages=10,
                       requires_spatial_filter=True,
                       spatial_params={"dept": "code_departement"},
                       cache_duration=30,
                       depth_limit=50000  # Limite élevée pour éviter troncature
                   ),
                   "operation_pc": HubeauEndpointConfig(
                       path="operation_pc",
                       temporal_params={"start": "date_debut_prelevement", "end": "date_fin_prelevement"},
                       page_size=2000,
                       max_pages=10,
                       requires_spatial_filter=True,
                       spatial_params={"dept": "code_departement"},
                       cache_duration=30,
                       depth_limit=100000  # Limite très élevée pour opérations
                   ),
                   "condition_environnementale_pc": HubeauEndpointConfig(
                       path="condition_environnementale_pc",
                       temporal_params={"start": "date_debut_prelevement", "end": "date_fin_prelevement"},
                       page_size=2000,
                       max_pages=10,
                       requires_spatial_filter=True,
                       spatial_params={"dept": "code_departement"},
                       cache_duration=30,
                       depth_limit=100000  # Limite très élevée pour conditions
                   ),
                   "analyse_pc": HubeauEndpointConfig(
                       path="analyse_pc",
                       temporal_params={"start": "date_debut_prelevement", "end": "date_fin_prelevement"},
                       page_size=2000,
                       max_pages=10,
                       requires_spatial_filter=True,
                       spatial_params={"dept": "code_departement"},
                       cache_duration=30,
                       depth_limit=100000  # Limite très élevée pour analyses
                   )
        }
    )

def get_ground_water_quality_config() -> HubeauApiConfig:
    """Configuration API Qualité Nappes"""
    return HubeauApiConfig(
        name="ground_water_quality",
        base_url="https://hubeau.eaufrance.fr/api/v1/qualite_nappes",
        version="v1",
        endpoints={
                   "stations": HubeauEndpointConfig(
                       path="stations",
                       page_size=5000,
                       max_pages=10,
                       requires_spatial_filter=True,
                       spatial_params={"dept": "num_departement"},
                       cache_duration=30,
                       depth_limit=50000  # Limite élevée pour éviter troncature
                   ),
                   "analyses": HubeauEndpointConfig(
                       path="analyses",
                       temporal_params={"start": "date_debut_prelevement", "end": "date_fin_prelevement"},
                       page_size=1000,
                       max_pages=20,
                       requires_spatial_filter=True,
                       spatial_params={"dept": "num_departement"},
                       cache_duration=30,
                       depth_limit=100000  # Limite très élevée pour analyses
                   )
        }
    )

def get_temperature_config() -> HubeauApiConfig:
    """Configuration API Température"""
    return HubeauApiConfig(
        name="temperature",
        base_url="https://hubeau.eaufrance.fr/api/v1/temperature",
        version="v1",
        endpoints={
                   "station": HubeauEndpointConfig(
                       path="station",
                       page_size=5000,
                       max_pages=10,
                       cache_duration=30,
                       depth_limit=50000  # Limite élevée pour éviter troncature
                   ),
                   "chronique": HubeauEndpointConfig(
                       path="chronique",
                       temporal_params={"start": "date_debut_mesure", "end": "date_fin_mesure"},
                       page_size=1000,
                       max_pages=20,
                       cache_duration=30,
                       depth_limit=100000  # Limite très élevée pour chroniques
                   )
        }
    )

def get_onde_config() -> HubeauApiConfig:
    """Configuration API ONDE (Écoulement)"""
    return HubeauApiConfig(
        name="onde",
        base_url="https://hubeau.eaufrance.fr/api/v1/ecoulement",
        version="v1",
        endpoints={
                   "stations": HubeauEndpointConfig(
                       path="stations",
                       page_size=5000,
                       max_pages=10,
                       cache_duration=30,
                       depth_limit=50000  # Limite élevée pour éviter troncature
                   ),
                   "campagnes": HubeauEndpointConfig(
                       path="campagnes",
                       temporal_params={"start": "date_debut_campagne", "end": "date_fin_campagne"},
                       page_size=1000,
                       max_pages=20,
                       cache_duration=30,
                       depth_limit=100000  # Limite très élevée pour campagnes
                   ),
                   "observations": HubeauEndpointConfig(
                       path="observations",
                       temporal_params={"start": "date_debut_observation", "end": "date_fin_observation"},
                       page_size=1000,
                       max_pages=20,
                       cache_duration=30,
                       depth_limit=100000  # Limite très élevée pour observations
                   )
        }
    )

def get_hydrobiology_config() -> HubeauApiConfig:
    """Configuration API Hydrobiologie"""
    return HubeauApiConfig(
        name="hydrobiology",
        base_url="https://hubeau.eaufrance.fr/api/v1/hydrobio",
        version="v1",
        max_retries=5,  # ✅ CORRECTIF C: Plus de retries pour API sensible
        rate_limit_delay=0.6,  # ✅ CORRECTIF C: Rate limit plus respectueux
        endpoints={
                   "stations_hydrobio": HubeauEndpointConfig(
                       path="stations_hydrobio",
                       page_size=2000,
                       max_pages=10,
                       requires_spatial_filter=True,
                       spatial_params={"dept": "code_departement"},
                       cache_duration=30,
                       depth_limit=None  # ✅ CORRECTIF A: Pas de cap global, chaque requête respecte la pagination API
                   ),
                   "indices": HubeauEndpointConfig(
                       path="indices",
                       temporal_params={"start": "date_debut_prelevement", "end": "date_fin_prelevement"},
                       page_size=500,
                       max_pages=20,
                       requires_spatial_filter=False,  # ✅ Filtrage par code_station_hydrobio (plus robuste)
                       cache_duration=30,
                       depth_limit=50000,  # Limite élevée pour indices
                       end_offset_days=1  # ✅ Borne fin exclusive : [J, J+1)
                   ),
                   "taxons": HubeauEndpointConfig(
                       path="taxons",
                       temporal_params={"start": "date_debut_prelevement", "end": "date_fin_prelevement"},
                       page_size=500,
                       max_pages=20,
                       requires_spatial_filter=True,
                       spatial_params={"dept": "code_departement"},
                       cache_duration=30,
                       depth_limit=50000,  # Limite élevée pour taxons
                       end_offset_days=1  # ✅ Borne fin exclusive : [J, J+1)
                   )
        }
    )

def get_prelevements_config() -> HubeauApiConfig:
    """Configuration API Prélèvements (corrigée selon legacy)"""
    return HubeauApiConfig(
        name="prelevements",
        base_url="https://hubeau.eaufrance.fr/api/v1/prelevements",
        version="v1",
        endpoints={
                   "points_prelevement": HubeauEndpointConfig(
                       path="referentiel/points_prelevement",  # CORRIGÉ: sous referentiel/
                       page_size=2000,
                       max_pages=10,
                       requires_spatial_filter=True,
                       spatial_params={"dept": "code_departement"},
                       cache_duration=30,
                       depth_limit=50000  # Limite élevée pour éviter troncature
                   ),
                   "chroniques": HubeauEndpointConfig(
                       path="chroniques",
                       temporal_params={"start": "annee_min", "end": "annee_max"},  # CORRIGÉ: paramètres temporels annuels
                       page_size=1000,
                       max_pages=20,
                       requires_spatial_filter=True,
                       spatial_params={"dept": "code_departement"},
                       cache_duration=30,
                       depth_limit=100000  # Limite très élevée pour chroniques
                   )
        }
    )

# ====================================
# UTILITAIRES
# ====================================

def get_hubeau_api_names() -> List[str]:
    """Retourne la liste des noms d'APIs Hub'Eau"""
    return list(get_all_hubeau_configs().keys())

def get_hubeau_endpoints_for_api(api_name: str) -> List[str]:
    """Retourne la liste des endpoints pour une API donnée"""
    configs = get_all_hubeau_configs()
    if api_name in configs:
        return list(configs[api_name].endpoints.keys())
    return []

def validate_hubeau_api(api_name: str) -> bool:
    """Valide qu'une API Hub'Eau existe"""
    return api_name in get_hubeau_api_names()
