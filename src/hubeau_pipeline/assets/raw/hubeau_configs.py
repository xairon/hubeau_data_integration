"""
Configurations Hub'Eau centralisées
Définition des paramètres pour chaque API Hub'Eau
"""

from typing import Dict, List

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
        "ecoulement": get_ecoulement_config(),
        "hydrobiology": get_hydrobiology_config(),
        "prelevements": get_prelevements_config()
    }

def get_hydrometry_config() -> HubeauApiConfig:
    """
    Configuration API Hydrométrie
    
    ⚠️ RESTRICTION CRITIQUE API v2 : Accès limité aux 30 derniers jours UNIQUEMENT
    - Erreur 400 si date_debut_obs < 30 jours
    - Pas d'accès à l'historique ancien
    - Utiliser obs_elab pour données élaborées (même restriction)
    """
    return HubeauApiConfig(
        name="hydrometry",
        base_url="https://hubeau.eaufrance.fr/api/v2/hydrometrie",
        version="v2",
        endpoints={
                   "referentiel_sites": HubeauEndpointConfig(
                       path="referentiel/sites",
                       cache_duration=30,
                   ),
            "referentiel_stations": HubeauEndpointConfig(
                path="referentiel/stations",
                cache_duration=30,
            ),
                   "observations_tr": HubeauEndpointConfig(
                       path="observations_tr",
                       temporal_params={"start": "date_debut_obs", "end": "date_fin_obs"},
                       supports_cursor=True,
                       realtime_cache_duration=15,
                       cache_duration=30,
                   ),
                   "obs_elab": HubeauEndpointConfig(
                       path="obs_elab",
                       temporal_params={"start": "date_debut_obs_elab", "end": "date_fin_obs_elab"},
                       supports_cursor=True,
                       cache_duration=30,
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
                       cache_duration=30,
                   ),
                   "chroniques_tr": HubeauEndpointConfig(
                       path="chroniques_tr",
                       temporal_params={"start": "date_debut_mesure", "end": "date_fin_mesure"},
                       realtime_cache_duration=15,
                       cache_duration=30,
                   ),
                   "chroniques": HubeauEndpointConfig(
                       path="chroniques",
                       temporal_params={"start": "date_debut_mesure", "end": "date_fin_mesure"},
                       cache_duration=30,
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
                       cache_duration=30,
                   ),
                   "operation_pc": HubeauEndpointConfig(
                       path="operation_pc",
                       temporal_params={"start": "date_debut_prelevement", "end": "date_fin_prelevement"},
                       cache_duration=30,
                   ),
                   "condition_environnementale_pc": HubeauEndpointConfig(
                       path="condition_environnementale_pc",
                       temporal_params={"start": "date_debut_prelevement", "end": "date_fin_prelevement"},
                       cache_duration=30,
                   ),
                   "analyse_pc": HubeauEndpointConfig(
                       path="analyse_pc",
                       temporal_params={"start": "date_debut_prelevement", "end": "date_fin_prelevement"},
                       cache_duration=30,
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
                       cache_duration=30,
                   ),
                   "analyses": HubeauEndpointConfig(
                       path="analyses",
                       temporal_params={"start": "date_debut_prelevement", "end": "date_fin_prelevement"},
                       cache_duration=30,
                   )
        }
    )

def get_temperature_config() -> HubeauApiConfig:
    """
    Configuration API Température des cours d'eau
    
    Selon la documentation officielle: https://hubeau.eaufrance.fr/page/api-temperature-continu
    - ~760 stations de mesure (dont ~50 encore en service)
    - Données mises à jour trimestriellement depuis Naïades
    - Limite de profondeur: 20 000 enregistrements par requête
    - Filtrage spatial: code_departement pour stations ET chroniques
    """
    return HubeauApiConfig(
        name="temperature",
        base_url="https://hubeau.eaufrance.fr/api/v1/temperature",
        version="v1",
        max_retries=8,
        rate_limit_delay=1.5,
        endpoints={
            "station": HubeauEndpointConfig(
                path="station",
                cache_duration=30,
            ),
            "chronique": HubeauEndpointConfig(
                path="chronique",
                temporal_params={"start": "date_debut_mesure", "end": "date_fin_mesure"},
                cache_duration=30,
            )
        }
    )

def get_ecoulement_config() -> HubeauApiConfig:
    """Configuration API Écoulement des cours d'eau (ONDE)"""
    return HubeauApiConfig(
        name="ecoulement",
        base_url="https://hubeau.eaufrance.fr/api/v1/ecoulement",
        version="v1",
        max_retries=5,  # ✅ CORRECTIF: Plus de retries pour API sensible
        rate_limit_delay=0.7,  # ✅ CORRECTIF: Rate limit plus respectueux pour éviter erreurs 500
        endpoints={
                   "stations": HubeauEndpointConfig(
                       path="stations",
                       cache_duration=30,
                   ),
                   "campagnes": HubeauEndpointConfig(
                       path="campagnes",
                       temporal_params={"start": "date_debut_campagne", "end": "date_fin_campagne"},
                       cache_duration=30,
                   ),
                   "observations": HubeauEndpointConfig(
                       path="observations",
                       temporal_params={"start": "date_observation_min", "end": "date_observation_max"},
                       cache_duration=30,
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
                       cache_duration=30,
                   ),
                   "indices": HubeauEndpointConfig(
                       path="indices",
                       temporal_params={"start": "date_debut_prelevement", "end": "date_fin_prelevement"},
                       cache_duration=30,
                       end_offset_days=1
                   ),
                   "taxons": HubeauEndpointConfig(
                       path="taxons",
                       temporal_params={"start": "date_debut_prelevement", "end": "date_fin_prelevement"},
                       cache_duration=30,
                       end_offset_days=1
                   )
        }
    )

def get_prelevements_config() -> HubeauApiConfig:
    """Configuration API Prélèvements (complète avec les 3 endpoints)"""
    return HubeauApiConfig(
        name="prelevements",
        base_url="https://hubeau.eaufrance.fr/api/v1/prelevements",
        version="v1",
        max_retries=8,  # ✅ CORRECTIF: Plus de retries pour API sensible aux erreurs 500
        rate_limit_delay=1.0,  # ✅ CORRECTIF: Rate limit plus respectueux pour éviter erreurs 500
        endpoints={
            "points_prelevement": HubeauEndpointConfig(
                path="referentiel/points_prelevement",
                cache_duration=30,
            ),
            "ouvrages": HubeauEndpointConfig(
                path="referentiel/ouvrages",
                cache_duration=30,
            ),
            "chroniques": HubeauEndpointConfig(
                path="chroniques",
                temporal_params={"start": "annee_min", "end": "annee_max"},
                cache_duration=30,
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
