"""
Configurations Hub'Eau centralisées avec hyperparamètres communs
Chaque API a des paramètres spécifiques selon sa documentation officielle
"""

from typing import Dict
from .hubeau_real_ingestion import HubeauAPIConfig, EndpointConfig, DeduplicationConfig

# ====================================
# HYPERPARAMÈTRES CENTRALISÉS
# ====================================

# Configuration temporelle
DAILY_LOOKBACK_DAYS = 1  # Cohérent avec partitions quotidiennes
DEFAULT_LOOKBACK_DAYS = 365  # Pour les référentiels

# Configuration pagination
DEFAULT_PAGE_SIZE = 1000
LARGE_PAGE_SIZE = 2000
REFERENTIEL_PAGE_SIZE = 5000
MAX_PAGE_SIZE = 20000

# Configuration profondeur
DEFAULT_DEPTH_LIMIT = 20000
HYDROBIOLOGIE_DEPTH_LIMIT = 10000  # Spécifique hydrobiologie

# Configuration retry/timeout
DEFAULT_MAX_RETRIES = 3
DEFAULT_TIMEOUT = 120
LONG_TIMEOUT = 180

# Configuration rate limiting
DEFAULT_RATE_LIMIT_DELAY = 0.5  # 2 req/sec max Hub'Eau

# Configuration déduplication
DEFAULT_DEDUP_TRUNCATE_TO_DAY = True


def get_hubeau_piezo_config() -> HubeauAPIConfig:
    """Configuration API Piézométrie Hub'Eau - CORRIGÉE selon doc officielle"""
    return HubeauAPIConfig(
        name="piezo",
        base_url="https://hubeau.eaufrance.fr/api/v1/niveaux_nappes",
        version="v1",
        # PAS de filtre spatial requis pour Piézométrie selon doc officielle
        endpoints={
            "stations": EndpointConfig(
                path="stations",
                apply_temporal_filter=False,
                page_size=LARGE_PAGE_SIZE,
                max_page_size=MAX_PAGE_SIZE,
                depth_limit=DEFAULT_DEPTH_LIMIT,
                spatial_filter_required=True, # Forcer le chunking spatial
            ),
            "chroniques_tr": EndpointConfig(
                path="chroniques_tr",
                temporal_param_keys=("date_debut_mesure", "date_fin_mesure"),
                # ✅ lookback_days supprimé - utilise uniquement la date de partition
                page_size=LARGE_PAGE_SIZE,
                max_page_size=LARGE_PAGE_SIZE,
                depth_limit=DEFAULT_DEPTH_LIMIT,
                spatial_filter_required=True,  # Activer chunking spatial
                deduplication=DeduplicationConfig(
                    date_field="date_mesure",
                    group_keys=["code_bss"],
                    truncate_to_day=DEFAULT_DEDUP_TRUNCATE_TO_DAY,
                ),
            ),
            "chroniques": EndpointConfig(
                path="chroniques",
                temporal_param_keys=("date_debut_mesure", "date_fin_mesure"),
                apply_temporal_filter=True,  # CORRIGÉ: chroniques nécessitent filtres temporels
                # ✅ lookback_days supprimé - utilise uniquement la date de partition
                page_size=DEFAULT_PAGE_SIZE,
                max_page_size=DEFAULT_PAGE_SIZE,
                depth_limit=DEFAULT_DEPTH_LIMIT,
                spatial_filter_required=True,  # CORRIGÉ: activer chunking spatial pour éviter limite 20k
                deduplication=DeduplicationConfig(
                    date_field="date_mesure",
                    group_keys=["code_bss"],
                    truncate_to_day=DEFAULT_DEDUP_TRUNCATE_TO_DAY,
                ),
            ),
        },
        base_params={"format": "json"},
        max_retries=DEFAULT_MAX_RETRIES,
        timeout=DEFAULT_TIMEOUT,
        rate_limit_delay=DEFAULT_RATE_LIMIT_DELAY,
        default_lookback_days=DEFAULT_LOOKBACK_DAYS,
    )


def get_hubeau_hydro_config() -> HubeauAPIConfig:
    """Configuration API Hydrométrie Hub'Eau v2 - CORRIGÉE avec pagination cursor"""
    return HubeauAPIConfig(
        name="hydro",
        base_url="https://hubeau.eaufrance.fr/api/v2/hydrometrie",
        version="v2",
        # PAS de filtre spatial requis pour Hydrométrie selon doc officielle
        endpoints={
            "referentiel_sites": EndpointConfig(
                path="referentiel/sites",  # AJOUTÉ: Référentiel des sites hydrométriques
                apply_temporal_filter=False,
                page_size=5000,  # Référentiels peuvent avoir des pages plus grandes
                max_page_size=20000,
                depth_limit=20000,  # Profondeur max pour éviter troncature
                pagination_mode="page",  # Référentiels utilisent la pagination classique
            ),
            "referentiel_stations": EndpointConfig(
                path="referentiel/stations",
                apply_temporal_filter=False,
                page_size=5000,  # Référentiels peuvent avoir des pages plus grandes
                max_page_size=20000,
                depth_limit=20000,  # Profondeur max pour éviter troncature
                pagination_mode="page",  # Référentiels utilisent la pagination classique
            ),
            "observations_tr": EndpointConfig(
                path="observations_tr",
                temporal_param_keys=("date_debut_obs", "date_fin_obs"),
                # ✅ lookback_days supprimé - utilise uniquement la date de partition
                page_size=1000,  # size reste utile même avec cursor
                max_page_size=5000,  # Hydrométrie v2 peut supporter plus
                depth_limit=50000,  # ✅ CORRIGÉ: Limite raisonnable pour éviter pagination excessive
                pagination_mode="cursor",  # v2 utilise la pagination cursor
                supports_sort=False,  # observations_tr utilise l'ordre via cursor
                deduplication=DeduplicationConfig(
                    date_field="date_obs",
                    group_keys=["code_station", "code_site"],  # Inclure code_site pour v2
                ),
            ),
            "obs_elab": EndpointConfig(
                path="obs_elab",  # AJOUTÉ: Observations élaborées/validées
                # ✅ v2 → bons paramètres temporels
                temporal_param_keys=("date_debut_obs_elab", "date_fin_obs_elab"),
                # ✅ lookback_days supprimé - utilise uniquement la date de partition
                page_size=1000,  # Optimisé selon limite
                max_page_size=5000,  # Hydrométrie v2 peut supporter plus
                depth_limit=20000,  # ✅ CORRIGÉ: Limite raisonnable pour éviter pagination excessive
                # ✅ v2 → pagination par page (pas cursor)
                pagination_mode="page",
                # on peut trier côté client si l'API ne garantit pas le tri
                supports_sort=True,
                # ✅ bons champs de dédup
                deduplication=DeduplicationConfig(
                    date_field="date_obs_elab",
                    # code_station peut être null en v2 (obs portées par le site) → inclure code_site
                    group_keys=["code_site", "code_station", "grandeur_hydro_elab"],
                    truncate_to_day=False,
                ),
            ),
        },
        base_params={"format": "json"},
        max_retries=3,
        timeout=120,
        default_lookback_days=1,  # ✅ CORRIGÉ: Cohérent avec partitions quotidiennes
    )


def get_hubeau_quality_surface_config() -> HubeauAPIConfig:
    """Configuration API Qualité Cours d'Eau Hub'Eau v2 - CORRIGÉE selon doc officielle"""
    return HubeauAPIConfig(
        name="quality_surface",
        base_url="https://hubeau.eaufrance.fr/api/v2/qualite_rivieres",
        version="v2",
        requires_spatial_filter=True,
        endpoints={
            "station_pc": EndpointConfig(
                path="station_pc",
                apply_temporal_filter=False,
                page_size=5000,  # Référentiels peuvent être plus volumineux
                max_page_size=20000,
                depth_limit=20000,  # Profondeur max pour éviter troncature
                spatial_filter_required=True,
                supports_sort=False,  # station_pc ne supporte pas le tri
            ),
            "operation_pc": EndpointConfig(
                path="operation_pc",  # AJOUTÉ: Opérations de prélèvement
                temporal_param_keys=("date_debut_prelevement", "date_fin_prelevement"),
                temporal_format="%Y-%m-%d",
                # ✅ lookback_days supprimé - utilise uniquement la date de partition
                page_size=2000,  # Optimisé selon limite
                max_page_size=20000,
                depth_limit=20000,  # Profondeur max pour éviter troncature
                spatial_filter_required=True,
                supports_sort=True,  # operation_pc supporte le tri selon la doc
                deduplication=DeduplicationConfig(
                    date_field="date_prelevement",
                    group_keys=["code_station", "code_operation"],
                ),
            ),
            "condition_environnementale_pc": EndpointConfig(
                path="condition_environnementale_pc",  # AJOUTÉ: Conditions environnementales
                temporal_param_keys=("date_debut_prelevement", "date_fin_prelevement"),
                temporal_format="%Y-%m-%d",
                # ✅ lookback_days supprimé - utilise uniquement la date de partition
                page_size=2000,  # Optimisé selon limite
                max_page_size=20000,
                depth_limit=20000,  # Profondeur max pour éviter troncature
                spatial_filter_required=True,
                supports_sort=True,  # condition_environnementale_pc supporte le tri selon la doc
                deduplication=DeduplicationConfig(
                    date_field="date_prelevement",
                    group_keys=["code_station", "code_operation"],
                ),
            ),
            "analyse_pc": EndpointConfig(
                path="analyse_pc",
                temporal_param_keys=("date_debut_prelevement", "date_fin_prelevement"),
                temporal_format="%Y-%m-%d",
                # ✅ lookback_days supprimé - utilise uniquement la date de partition
                page_size=2000,  # Réduire pour forcer plus de subdivision
                max_page_size=20000,
                depth_limit=20000,  # Profondeur max pour éviter troncature
                spatial_filter_required=True,
                supports_sort=True,  # analyse_pc supporte le tri selon la doc
                deduplication=DeduplicationConfig(
                    date_field="date_prelevement",
                    group_keys=["code_station", "code_parametre"],
                ),
            ),
        },
        base_params={"format": "json"},
        max_retries=3,
        timeout=180,
        default_lookback_days=1,  # ✅ CORRIGÉ: Cohérent avec partitions quotidiennes
    )


def get_hubeau_quality_groundwater_config() -> HubeauAPIConfig:
    """Configuration API Qualité Nappes Hub'Eau v1 corrigée"""
    return HubeauAPIConfig(
        name="quality_groundwater",
        base_url="https://hubeau.eaufrance.fr/api/v1/qualite_nappes",
        version="v1",
        requires_spatial_filter=True,
        endpoints={
            "stations": EndpointConfig(
                path="stations",
                apply_temporal_filter=False,
                page_size=5000,  # Référentiels optimisés
                max_page_size=20000,
                depth_limit=20000,  # Profondeur max pour éviter troncature
                spatial_filter_required=True,
                spatial_dept_param="num_departement",  # Clé spécifique Qualité nappes
            ),
            "analyses": EndpointConfig(
                path="analyses",
                temporal_param_keys=("date_debut_prelevement", "date_fin_prelevement"),
                # ✅ lookback_days supprimé - utilise uniquement la date de partition
                page_size=1000,  # Optimisé selon limite 20k
                max_page_size=1000,
                depth_limit=20000,  # Profondeur max pour éviter troncature
                spatial_filter_required=True,
                spatial_dept_param="num_departement",  # Clé spécifique Qualité nappes
                supports_sort=True,  # Parfois supporté selon doc
                deduplication=DeduplicationConfig(
                    date_field="date_debut_prelevement",
                    group_keys=["code_bss", "code_parametre"],
                ),
            ),
        },
        base_params={"format": "json"},
        max_retries=3,
        timeout=180,
        default_lookback_days=1,  # ✅ CORRIGÉ: Cohérent avec partitions quotidiennes
    )


def get_hubeau_temperature_config() -> HubeauAPIConfig:
    """Configuration API Température Hub'Eau v1 - CORRIGÉE selon doc officielle"""
    return HubeauAPIConfig(
        name="temperature",
        base_url="https://hubeau.eaufrance.fr/api/v1/temperature",
        version="v1",
        # Filtre spatial recommandé mais pas obligatoire selon doc
        endpoints={
            "station": EndpointConfig(
                path="station",
                apply_temporal_filter=False,
                page_size=5000,  # Référentiels optimisés
                max_page_size=20000,
                depth_limit=20000,  # Profondeur max pour éviter troncature
            ),
            "chronique": EndpointConfig(
                path="chronique",
                temporal_param_keys=("date_debut_mesure", "date_fin_mesure"),
                # ✅ lookback_days supprimé - utilise uniquement la date de partition
                page_size=1000,  # Optimisé selon limite 20k
                max_page_size=1000,
                depth_limit=20000,  # Profondeur max pour éviter troncature
                supports_sort=False,  # Température ne supporte pas le tri
                deduplication=DeduplicationConfig(
                    date_field="date_mesure_temp",  # CORRIGÉ: champ spécifique température
                    group_keys=["code_station", "heure_mesure_temp"],  # CORRIGÉ: granularité horaire
                    truncate_to_day=False,  # CORRIGÉ: ne pas tronquer au jour pour préserver granularité horaire
                ),
            ),
        },
        base_params={"format": "json"},
        max_retries=3,
        timeout=120,
        default_lookback_days=1,  # ✅ CORRIGÉ: Cohérent avec partitions quotidiennes
    )


def get_hubeau_onde_config() -> HubeauAPIConfig:
    """Configuration API ONDE (Écoulement) Hub'Eau v1 - CORRIGÉE selon REX"""
    return HubeauAPIConfig(
        name="onde",
        base_url="https://hubeau.eaufrance.fr/api/v1/ecoulement",
        version="v1",
        # PAS de filtre spatial obligatoire pour ONDE selon doc
        endpoints={
            "stations": EndpointConfig(
                path="stations",
                apply_temporal_filter=False,
                page_size=5000,  # Référentiels optimisés
                max_page_size=20000,  # Selon doc Hub'Eau
                depth_limit=20000,  # Profondeur max pour éviter troncature
            ),
            "campagnes": EndpointConfig(
                path="campagnes",
                temporal_param_keys=("date_campagne_min", "date_campagne_max"),
                # ✅ lookback_days supprimé - utilise uniquement la date de partition
                page_size=1000,
                max_page_size=1000,
                depth_limit=20000,  # Profondeur max pour éviter troncature
                supports_sort=False,
            ),
            "observations": EndpointConfig(
                path="observations",
                temporal_param_keys=("date_observation_min", "date_observation_max"),
                # ✅ lookback_days supprimé - utilise uniquement la date de partition
                page_size=5000,  # Augmenter pour éviter troncature ONDE
                max_page_size=20000,  # Max selon doc
                depth_limit=20000,  # Profondeur max pour éviter troncature
                supports_sort=False,  # ONDE observations ne supporte pas le tri
                deduplication=DeduplicationConfig(
                    date_field="date_observation",
                    group_keys=["code_station"],
                    truncate_to_day=True,
                ),
            ),
        },
        base_params={"format": "json"},
        max_retries=3,
        timeout=180,
        default_lookback_days=1,  # ✅ CORRIGÉ: Cohérent avec partitions quotidiennes
    )


def get_hubeau_hydrobiologie_config() -> HubeauAPIConfig:
    """Configuration API Hydrobiologie Hub'Eau v1 - CORRIGÉE selon REX"""
    return HubeauAPIConfig(
        name="hydrobiologie",
        base_url="https://hubeau.eaufrance.fr/api/v1/hydrobio",
        version="v1",
        # PAS de filtre spatial obligatoire pour Hydrobiologie selon doc
        endpoints={
            "stations_hydrobio": EndpointConfig(
                path="stations_hydrobio",
                apply_temporal_filter=False,
                page_size=2000,
                max_page_size=10000,
                depth_limit=10000,
                spatial_filter_required=True, # Forcer le chunking spatial
            ),
            "indices": EndpointConfig(
                path="indices",
                temporal_param_keys=("date_debut_prelevement", "date_fin_prelevement"),
                # ✅ lookback_days supprimé - utilise uniquement la date de partition
                page_size=500,
                max_page_size=500,
                depth_limit=10000,
                spatial_filter_required=True,
            ),
            "taxons": EndpointConfig(
                path="taxons",
                temporal_param_keys=("date_debut_prelevement", "date_fin_prelevement"),
                # ✅ lookback_days supprimé - utilise uniquement la date de partition
                page_size=500,
                max_page_size=500,
                depth_limit=10000,
                spatial_filter_required=True,
            ),
        },
        base_params={"format": "json"},
        max_retries=3,
        timeout=180,
        default_lookback_days=1,  # ✅ CORRIGÉ: Cohérent avec partitions quotidiennes
    )


def get_hubeau_prelevements_config() -> HubeauAPIConfig:
    """Configuration API Prélèvements Hub'Eau v1 corrigée"""
    return HubeauAPIConfig(
        name="prelevements",
        base_url="https://hubeau.eaufrance.fr/api/v1/prelevements",
        version="v1",
        requires_spatial_filter=True,
        endpoints={
            "points_prelevement": EndpointConfig(
                path="referentiel/points_prelevement",  # Référentiel sous referentiel/
                apply_temporal_filter=False,
                page_size=2000,  # Réduire pour éviter profondeur 20k
                max_page_size=20000,
                depth_limit=20000,  # Profondeur max pour éviter troncature
                spatial_filter_required=True,
            ),
            "ouvrages": EndpointConfig(
                path="referentiel/ouvrages",  # AJOUTÉ: Référentiel des ouvrages
                apply_temporal_filter=False,
                page_size=2000,  # Réduire pour éviter profondeur 20k
                max_page_size=20000,
                depth_limit=20000,  # Profondeur max pour éviter troncature
                spatial_filter_required=True,
            ),
            "chroniques": EndpointConfig(
                path="chroniques",  # Chroniques restent à la racine
                temporal_param_keys=("annee_min", "annee_max"),  # CORRIGÉ: paramètres temporels pour prélèvements
                temporal_format="%Y",  # CORRIGÉ: format année pour prélèvements
                apply_temporal_filter=True,  # CORRIGÉ: activer filtres temporels
                # ✅ lookback_days supprimé - utilise uniquement la date de partition
                page_size=1000,  # Optimisé selon limite 20k
                max_page_size=1000,
                depth_limit=20000,  # Profondeur max pour éviter troncature
                spatial_filter_required=True,         # <— important
                spatial_dept_param="code_departement",
                supports_sort=False,  # Prélèvements ne supporte pas le tri
                deduplication=DeduplicationConfig(
                    date_field="annee",  # Chroniques annuelles selon la doc
                    group_keys=["code_ouvrage"],  # code_ouvrage selon la doc
                    truncate_to_day=False,  # Prélèvements par période, pas par jour
                ),
            ),
        },
        base_params={"format": "json"},
        max_retries=3,
        timeout=180,
        default_lookback_days=1,  # ✅ CORRIGÉ: Cohérent avec partitions quotidiennes
    )


# Mapping pour accès facile
HUBEAU_CONFIGS = {
    "piezo": get_hubeau_piezo_config,
    "hydro": get_hubeau_hydro_config,
    "quality_surface": get_hubeau_quality_surface_config,
    "quality_groundwater": get_hubeau_quality_groundwater_config,
    "temperature": get_hubeau_temperature_config,
    "onde": get_hubeau_onde_config,
    "hydrobiologie": get_hubeau_hydrobiologie_config,
    "prelevements": get_hubeau_prelevements_config,
}
