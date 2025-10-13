"""
Sources DLT natives pour Hub'Eau avec configuration YAML
"""
import dlt
from dlt.sources.helpers import requests
from typing import Iterator, Optional, Dict, Any, List
import yaml
import os
import gc
import time
import logging
from datetime import datetime
from pathlib import Path

from .http_client import HttpClient
from .slicing import build_slices
from .transformers import apply_transformers

logger = logging.getLogger(__name__)


@dlt.source(name="hubeau", max_table_nesting=2)
def hubeau_source(
    config_path: str = None,
    config_dict: Dict = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    **runtime_params
):
    """
    Source DLT native pour Hub'Eau avec configuration YAML.

    Args:
        config_path: Chemin vers le fichier YAML
        config_dict: Ou directement un dict de config
        start_date: Date de début (format YYYY-MM-DD)
        end_date: Date de fin (format YYYY-MM-DD)
        **runtime_params: Paramètres runtime additionnels

    Returns:
        Resource DLT configurée

    Examples:
        # Depuis un fichier YAML
        source = hubeau_source(
            config_path="configs/hubeau/hydrometry_stations.yml"
        )

        # Avec dates
        source = hubeau_source(
            config_path="configs/hubeau/temperature_chroniques.yml",
            start_date="2024-01-01",
            end_date="2024-12-31"
        )

        # Depuis un dict
        source = hubeau_source(config_dict=my_config)
    """

    # Charger la configuration
    if config_path:
        config = load_config(config_path)
    elif config_dict:
        config = config_dict
    else:
        raise ValueError("Either config_path or config_dict must be provided")

    # Extraire les sections de config
    source_config = config.get("source", {})
    resource_config = config["resource"]
    extraction_config = config["extraction"]
    performance_config = config.get("performance", {})
    transformer_configs = config.get("transformers", [])

    # Créer la resource DLT
    @dlt.resource(
        name=resource_config["name"],
        primary_key=resource_config.get("primary_key", []),
        write_disposition=resource_config.get("write_disposition", "merge"),
        columns=resource_config.get("columns", {}),
        max_table_nesting=source_config.get("max_table_nesting", 2)
    )
    def api_resource() -> Iterator[Dict]:
        """
        Resource DLT avec state management et extraction optimisée.
        """
        # Récupérer le state DLT natif
        state = dlt.current.resource_state()
        last_value = state.setdefault("last_value", {})

        # Logger le début
        logger.info(f"Starting extraction for {resource_config['name']}")
        logger.info(f"Date range: {start_date or 'beginning'} to {end_date or 'now'}")

        # Créer le client HTTP
        http_config = {
            "base_url": resource_config["base_url"],
            "max_attempts": performance_config.get("retry_times", 3),
            "backoff_initial": performance_config.get("retry_delay", 2.0),
            "rate_limit": {
                "target_rps": performance_config.get("rate_limit", 2.0)
                if isinstance(performance_config.get("rate_limit"), (int, float))
                else performance_config.get("rate_limit", {}).get("target_rps", 2.0)
            }
        }
        http_client = HttpClient(http_config)

        # Configuration complète pour le slicing
        full_config = {
            **config,
            "path": resource_config.get("endpoint", "/"),
            "name": resource_config["name"],
            "base_url": resource_config["base_url"],
            "slicer": extraction_config.get("slicer", {}),
            "primary_keys": resource_config.get("primary_key", [])
        }

        # Créer les slices selon le mode de slicing
        slices = list(build_slices(full_config))
        logger.info(f"Created {len(slices)} slices with mode {extraction_config.get('slicing_mode', 'global')}")

        # Stats pour logging
        total_records = 0
        total_slices = 0
        start_time = time.time()

        # Extraire les données par slice
        for slice_idx, slice_obj in enumerate(slices):
            try:
                logger.info(f"Processing slice {slice_idx + 1}/{len(slices)}: {slice_obj.params}")

                # Extraire les données du slice
                slice_data = extract_slice(
                    http_client=http_client,
                    endpoint=resource_config.get("endpoint", resource_config.get("path", "/")),
                    slice_params=slice_obj.params,
                    extraction_config=extraction_config,
                    performance_config=performance_config,
                    base_config=full_config
                )

                # Compter avant transformation
                slice_records = list(slice_data)
                records_count = len(slice_records)

                if records_count > 0:
                    # Appliquer les transformers
                    transformed = apply_transformers(
                        iter(slice_records),
                        transformer_configs,
                        api_name=resource_config["name"]
                    )

                    # Yield les données transformées
                    for record in transformed:
                        # Ajouter metadata
                        record["_slice_id"] = slice_obj.slice_id
                        record["_scope"] = slice_obj.scope
                        yield record
                        total_records += 1

                    # Mettre à jour le state après chaque slice réussi
                    update_state(state, slice_obj.params, records_count)

                    logger.info(f"Slice {slice_idx + 1} completed: {records_count} records")

                total_slices += 1

                # Garbage collection après chaque slice pour éviter OOM
                if performance_config.get("enable_gc", True):
                    gc.collect()

            except Exception as e:
                logger.error(f"Error processing slice {slice_idx + 1}: {e}")
                if not extraction_config.get("continue_on_error", True):
                    raise

        # Logger le résumé
        elapsed = time.time() - start_time
        logger.info(f"Extraction completed for {resource_config['name']}")
        logger.info(f"Total: {total_records} records from {total_slices} slices in {elapsed:.2f}s")

        # Sauvegarder l'état final
        if end_date:
            state["last_date"] = end_date

        # Fermer le client HTTP
        http_client.close()

    return api_resource


def extract_slice(
    http_client,
    endpoint: str,
    slice_params: Dict,
    extraction_config: Dict,
    performance_config: Dict,
    base_config: Dict
) -> Iterator[Dict]:
    """
    Extrait les données d'un slice avec pagination.

    Args:
        http_client: Client HTTP configuré
        endpoint: Endpoint API
        slice_params: Paramètres du slice
        extraction_config: Configuration d'extraction
        performance_config: Configuration de performance
        base_config: Configuration de base complète

    Yields:
        Records du slice
    """
    # Paramètres de base
    params = {
        **extraction_config.get("default_params", {}),
        **slice_params
    }

    # Configuration de pagination
    pagination_config = extraction_config.get("pagination", {})
    page_size = pagination_config.get("page_size", 20000)
    max_pages = pagination_config.get("max_pages")
    pagination_type = pagination_config.get("type", "page_based")

    params["size"] = page_size

    # Extraire avec pagination
    if pagination_type == "cursor_based":
        yield from extract_with_cursor_pagination(
            http_client, endpoint, params, max_pages, base_config
        )
    else:
        yield from extract_with_page_pagination(
            http_client, endpoint, params, max_pages, base_config
        )


def extract_with_page_pagination(
    http_client,
    endpoint: str,
    params: Dict,
    max_pages: Optional[int],
    base_config: Dict
) -> Iterator[Dict]:
    """
    Extraction avec pagination par pages.
    """
    page = 1
    total_fetched = 0

    while True:
        # Paramètres de la page
        page_params = {**params, "page": page}

        try:
            # Faire la requête
            response = http_client.request("GET", endpoint, params=page_params)

            if not response:
                break

            # Extraire les records
            data = http_client.extract_records(response, base_config.get("records_path", "$.data"))
            data_list = list(data)

            if not data_list:
                break

            # Yield les records
            for record in data_list:
                yield record
                total_fetched += 1

            # Vérifier si on a tout récupéré
            if len(data_list) < params.get("size", 20000):
                break

            # Vérifier la limite de pages
            if max_pages and page >= max_pages:
                logger.warning(f"Reached max pages limit: {max_pages}")
                break

            page += 1

            # Check for API truncation
            if response.get("truncated", False):
                logger.warning(f"API response truncated at page {page}")
                break

        except Exception as e:
            logger.error(f"Error fetching page {page}: {e}")
            if page == 1:
                raise
            break


def extract_with_cursor_pagination(
    http_client,
    endpoint: str,
    params: Dict,
    max_pages: Optional[int],
    base_config: Dict
) -> Iterator[Dict]:
    """
    Extraction avec pagination par curseur.
    """
    cursor = None
    pages_fetched = 0

    while True:
        # Paramètres avec curseur
        page_params = {**params}
        if cursor:
            page_params["cursor"] = cursor

        try:
            # Faire la requête
            response = http_client.request("GET", endpoint, params=page_params)

            if not response:
                break

            # Extraire les records
            data = http_client.extract_records(response, base_config.get("records_path", "$.data"))
            data_list = list(data)

            if not data_list:
                break

            # Yield les records
            for record in data_list:
                yield record

            # Récupérer le prochain curseur
            import jsonpath_ng
            cursor_path = "$.next"
            jsonpath_expr = jsonpath_ng.parse(cursor_path)
            matches = [match.value for match in jsonpath_expr.find(response)]

            if matches and matches[0]:
                # Extraire le cursor de l'URL
                from urllib.parse import urlparse, parse_qs
                next_url = matches[0]
                parsed = urlparse(next_url)
                query_params = parse_qs(parsed.query)
                cursor = query_params.get("cursor", [None])[0]
                if not cursor:
                    break
            else:
                break

            pages_fetched += 1

            # Vérifier la limite de pages
            if max_pages and pages_fetched >= max_pages:
                logger.warning(f"Reached max pages limit: {max_pages}")
                break

        except Exception as e:
            logger.error(f"Error with cursor pagination: {e}")
            break


def update_state(state: Dict, slice_params: Dict, records_count: int):
    """
    Met à jour le state après traitement d'un slice.

    Args:
        state: State DLT
        slice_params: Paramètres du slice
        records_count: Nombre de records traités
    """
    # Mettre à jour les compteurs
    state.setdefault("total_records", 0)
    state["total_records"] += records_count

    state.setdefault("slices_processed", 0)
    state["slices_processed"] += 1

    # Sauvegarder la dernière date si présente
    if "date_debut_mesure" in slice_params:
        state["last_date"] = slice_params["date_debut_mesure"]
    elif "date" in slice_params:
        state["last_date"] = slice_params["date"]

    # Timestamp de dernière mise à jour
    state["last_updated"] = datetime.now().isoformat()


def load_config(config_path: str) -> Dict:
    """
    Charge et valide la configuration YAML.

    Args:
        config_path: Chemin vers le fichier YAML

    Returns:
        Configuration parsée et validée

    Raises:
        ValueError: Si la configuration est invalide
    """
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # Validation du nouveau format
    errors = validate_config(config)
    if errors:
        raise ValueError(f"Invalid config {config_path.name}: {'; '.join(errors)}")

    return config


def validate_config(config: Dict) -> List[str]:
    """
    Valide le format de configuration.

    Args:
        config: Configuration à valider

    Returns:
        Liste des erreurs (vide si valide)
    """
    errors = []

    # Sections requises
    required_sections = ["source", "resource", "extraction"]
    for section in required_sections:
        if section not in config:
            errors.append(f"Missing required section: {section}")

    # Champs requis dans resource
    if "resource" in config:
        required_resource_fields = ["name", "endpoint", "base_url"]
        for field in required_resource_fields:
            if field not in config["resource"]:
                errors.append(f"Missing required field in resource: {field}")

    # Validation du slicing_mode
    if "extraction" in config:
        valid_slicing_modes = [
            "global", "datetime", "dept",
            "station_month_chunked", "dept_datetime"
        ]
        slicing_mode = config["extraction"].get("slicing_mode")
        if slicing_mode and slicing_mode not in valid_slicing_modes:
            errors.append(f"Invalid slicing_mode: {slicing_mode}")

    # Validation des transformers
    if "transformers" in config:
        valid_transformers = [
            "validate_primary_keys", "normalize_dates",
            "validate_coordinates", "reject_duplicates",
            "clean_text", "convert_types"
        ]
        for transformer in config["transformers"]:
            if isinstance(transformer, str):
                if transformer not in valid_transformers:
                    errors.append(f"Unknown transformer: {transformer}")
            elif isinstance(transformer, dict):
                transformer_name = list(transformer.keys())[0]
                if transformer_name not in valid_transformers:
                    errors.append(f"Unknown transformer: {transformer_name}")

    return errors


# Fonction de compatibilité pour l'ancien code
def run_pipeline(
    cfg: Any,
    context: Any = None,
    destination: str = "filesystem",
    bucket_url: str = None,
    credentials: Dict = None,
    **kwargs
) -> Any:
    """
    Fonction de compatibilité avec l'ancienne interface.

    Args:
        cfg: Configuration (dict ou path)
        context: Contexte Dagster (optionnel)
        destination: Type de destination
        bucket_url: URL du bucket pour filesystem destination
        credentials: Credentials pour la destination
        **kwargs: Arguments additionnels

    Returns:
        LoadInfo DLT
    """
    import dlt
    from .destinations import get_destination, get_destination_from_config

    # Gérer cfg qui peut être un dict ou un path
    if isinstance(cfg, (str, Path)):
        config = load_config(cfg)
    elif isinstance(cfg, dict):
        config = cfg
    else:
        # Si c'est un objet validé (ancien code), extraire le dict
        if hasattr(cfg, 'model_dump'):
            config = cfg.model_dump(mode="python")
        else:
            config = cfg

    # Créer la source
    source = hubeau_source(
        config_dict=config,
        **kwargs
    )

    # Créer la destination
    # Si bucket_url est fourni, utiliser filesystem avec les credentials fournis
    if bucket_url:
        dest_config = {
            "bucket_url": bucket_url,
            **kwargs  # Inclut file_format, layout, etc.
        }
        dest = dlt.destinations.filesystem(
            bucket_url=bucket_url,
            credentials=credentials,
            **{k: v for k, v in kwargs.items() if k in ["layout", "file_format"]}
        )
    else:
        # Sinon utiliser la config YAML
        dest = get_destination_from_config(config)

    # Créer et exécuter le pipeline
    dataset_name = kwargs.get("dataset_name") or config.get("destinations", {}).get(destination, {}).get("dataset_name", "bronze")
    pipeline = dlt.pipeline(
        pipeline_name=f"hubeau_{config['resource']['name']}",
        destination=dest,
        dataset_name=dataset_name
    )

    # Log si contexte Dagster
    if context:
        if hasattr(context, 'log'):
            context.log.info(f"Running pipeline for {config['resource']['name']}")

    # Exécuter
    info = pipeline.run(source)

    # Log les résultats
    if context and hasattr(context, 'log'):
        context.log.info(f"Pipeline completed: {info}")

    return info


def run_from_file(config_path: str | Path) -> Any:
    """
    Execute un pipeline depuis un fichier de configuration.

    Args:
        config_path: Chemin vers le fichier YAML

    Returns:
        LoadInfo DLT
    """
    return run_pipeline(config_path)


__all__ = ["run_pipeline", "run_from_file", "hubeau_source"]