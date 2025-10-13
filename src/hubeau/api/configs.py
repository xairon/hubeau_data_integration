"""
Module de gestion des configurations API Hub'Eau.
Centralise toutes les configurations des 8 APIs pour éviter la duplication.
"""

import yaml
from pathlib import Path
from typing import Dict, Any, Optional


def get_api_configs() -> Dict[str, Dict[str, Any]]:
    """
    Charge toutes les configurations API depuis le fichier YAML.

    Returns:
        Dict contenant les configurations de toutes les APIs
    """
    config_path = Path(__file__).parent / "configs.yaml"
    with open(config_path, encoding='utf-8') as f:
        config_data = yaml.safe_load(f)
    return config_data.get("apis", {})


def get_api_config(api_type: str) -> Dict[str, Any]:
    """
    Récupère la configuration pour une API spécifique.

    Args:
        api_type: Type d'API (temperature, hydrometry, piezometry, etc.)

    Returns:
        Dict contenant la configuration de l'API

    Raises:
        KeyError: Si l'API n'est pas configurée
    """
    configs = get_api_configs()
    if api_type not in configs:
        raise KeyError(f"Configuration non trouvée pour l'API: {api_type}")
    return configs[api_type]


def get_referentiel_config(api_type: str) -> Dict[str, Any]:
    """
    Récupère la configuration du référentiel (stations) pour une API.

    Args:
        api_type: Type d'API

    Returns:
        Dict contenant la configuration du référentiel
    """
    config_path = Path(__file__).parent / "configs.yaml"
    with open(config_path, encoding='utf-8') as f:
        config_data = yaml.safe_load(f)

    referentiels = config_data.get("referentiels", {})
    if api_type not in referentiels:
        raise KeyError(f"Configuration référentiel non trouvée pour: {api_type}")
    return referentiels[api_type]


def get_slicing_mode(api_type: str) -> str:
    """
    Récupère le mode de slicing pour une API.

    Args:
        api_type: Type d'API

    Returns:
        Mode de slicing (station_month_chunked, datetime, dept, etc.)
    """
    config_path = Path(__file__).parent / "configs.yaml"
    with open(config_path, encoding='utf-8') as f:
        config_data = yaml.safe_load(f)

    slicing_modes = config_data.get("slicing_modes", {})
    return slicing_modes.get(api_type, "datetime")  # Default to datetime


def get_limits() -> Dict[str, int]:
    """
    Récupère les limites configurées (max records, truncation, etc.).

    Returns:
        Dict contenant les différentes limites
    """
    config_path = Path(__file__).parent / "configs.yaml"
    with open(config_path, encoding='utf-8') as f:
        config_data = yaml.safe_load(f)

    return config_data.get("limits", {
        "max_records_per_request": 20000,
        "truncation_threshold": 10000,
        "fallback_threshold": 5000,
        "max_fallbacks": 10
    })


def get_all_api_types() -> list[str]:
    """
    Retourne la liste de toutes les APIs configurées.

    Returns:
        Liste des types d'API disponibles
    """
    return list(get_api_configs().keys())


def get_station_field(api_type: str) -> str:
    """
    Récupère le nom du champ station pour une API.

    Args:
        api_type: Type d'API

    Returns:
        Nom du champ station (code_station, code_bss, etc.)
    """
    config = get_api_config(api_type)
    return config.get("station_field", "code_station")


def get_date_params(api_type: str) -> tuple[str, str]:
    """
    Récupère les paramètres de date pour une API.

    Args:
        api_type: Type d'API

    Returns:
        Tuple (start_param, end_param)
    """
    config = get_api_config(api_type)
    return config.get("start_param"), config.get("end_param")


def is_yearly_filtering(api_type: str) -> bool:
    """
    Vérifie si une API utilise un filtrage annuel uniquement.

    Args:
        api_type: Type d'API

    Returns:
        True si l'API utilise un filtrage annuel (comme prelevements)
    """
    config = get_api_config(api_type)
    return config.get("special_filtering") == "yearly"