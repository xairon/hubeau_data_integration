"""Generic dlt pipeline that ingests HubEau endpoints defined via YAML."""
from __future__ import annotations

import os
import re
import time
import logging
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, Iterator, List, Optional

import dlt
from jsonpath_ng import parse
from dlt.destinations import filesystem
from dlt.sources import incremental

from .http_client import HttpClient
from .schema import validate_config
from .slicing import Slice, build_slices, generate_fallback_slices, needs_truncation
from .state import save_state_copy

# Configuration du logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# S'assurer que les logs sont visibles
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def _replace_templates(config: Dict[str, Any], partition_date: str) -> Dict[str, Any]:
    """Remplace les templates {{ partition_date }} dans la configuration."""
    import copy
    import json
    
    # Créer une copie profonde pour éviter de modifier l'original
    config_copy = copy.deepcopy(config)
    
    # Convertir en JSON string pour remplacer facilement
    config_str = json.dumps(config_copy)
    
    # Remplacer {{ partition_date }} par la vraie date
    config_str = config_str.replace('"{{ partition_date }}"', f'"{partition_date}"')
    
    # Reconvertir en dict
    return json.loads(config_str)


def _build_params(cfg: Dict[str, Any], slice_obj: Slice, page: int | None = None, cursor_value: str | None = None) -> Dict[str, Any]:
    params = dict(cfg.get("params_default", {}))
    params.update(slice_obj.params)
    pagination = cfg.get("pagination") or {}
    
    # Ajouter les filtres temporels si configurés (en plus du slicer)
    temporal_filter = cfg.get("temporal_filter")
    if temporal_filter:
        from datetime import date, timedelta
        start_param = temporal_filter.get("start_param")
        end_param = temporal_filter.get("end_param")
        start_date = temporal_filter.get("start_date")
        end_offset_days = temporal_filter.get("end_offset_days", 1)
        
        if start_param and start_date:
            params[start_param] = start_date
        if end_param:
            # Calculer la date de fin
            today = date.today()
            end_date = today - timedelta(days=end_offset_days)
            params[end_param] = end_date.isoformat()
    
    if pagination.get("type") == "cursor":
        # Pagination par cursor - seulement ajouter le cursor s'il existe
        if cursor_value:
            params[pagination.get("cursor_param", "cursor")] = cursor_value
        # Sinon, ne pas ajouter le paramètre cursor (premier appel)
        params[pagination.get("page_size_param", "size")] = pagination.get("page_size", 500)
    elif pagination.get("type") == "page" and page is not None:
        # Pagination par page
        params[pagination.get("page_param", "page")] = page
        params[pagination.get("page_size_param", "size")] = pagination.get("page_size", 500)
    
    return params


_UNTIL_EXPR_RE = re.compile(r"len\((?P<path>[^)]+)\)\s*(?P<op><=|>=|==|!=|<|>)\s*(?P<value>\d+)")


def _evaluate_until_expr(expr: str, payload: Dict[str, Any]) -> bool:
    match = _UNTIL_EXPR_RE.fullmatch(expr.strip())
    if not match:
        raise ValueError(f"Unsupported until_expr: {expr}")
    path = match.group("path").strip()
    operator = match.group("op")
    value = int(match.group("value"))
    matches = parse(path).find(payload)
    if not matches:
        length = 0
    else:
        candidate = matches[0].value
        if isinstance(candidate, list):
            length = len(candidate)
        elif candidate is None:
            length = 0
        else:
            length = 1

    if operator == "<":
        return length < value
    if operator == "<=":
        return length <= value
    if operator == ">":
        return length > value
    if operator == ">=":
        return length >= value
    if operator == "==":
        return length == value
    if operator == "!=":
        return length != value
    raise ValueError(f"Unsupported operator in until_expr: {operator}")


def _should_stop_pagination(
    payload: Dict[str, Any],
    records: List[Dict[str, Any]],
    pagination: Dict[str, Any],
) -> bool:
    if not pagination:
        return True
    page_size = pagination.get("page_size")
    if page_size is not None and len(records) < page_size:
        return True
    until_expr = pagination.get("until_expr")
    if until_expr:
        try:
            if _evaluate_until_expr(until_expr, payload):
                return True
        except ValueError:
            # Invalid expressions are ignored but surfaced via debug logs if needed
            pass
    return False


@dlt.source(name="hubeau")
def hubeau_source(
    cfg: Dict[str, Any],
    client: Optional[HttpClient] = None,
    dagster_log: Optional[logging.Logger] = None,
    stations_data: Optional[list[str]] = None
) -> dlt.sources:
    """dlt source for the Hub'Eau APIs."""
    validated_cfg = validate_config(cfg)
    resolved_cfg = validated_cfg.model_dump(mode="python")

    http_client = client or HttpClient(resolved_cfg)
    owns_client = client is None

    # Utiliser merge pour chargement incrémental si replication_key est définie
    write_disposition = "merge" if validated_cfg.replication_key else "append"
    
    # Configuration du chargement incrémental natif DLT
    resource_kwargs = {
        "name": validated_cfg.name,
        "write_disposition": write_disposition,
        "primary_key": validated_cfg.primary_keys,
    }
    
    # Ajouter le chargement incrémental si replication_key est définie
    if validated_cfg.replication_key:
        # DLT gère automatiquement l'état incrémental
        # On ne charge que les données plus récentes que la dernière exécution
        resource_kwargs["merge_key"] = validated_cfg.primary_keys
    
    @dlt.resource(**resource_kwargs)
    def stream() -> Iterator[Dict[str, Any]]:
        pagination = resolved_cfg.get("pagination") or {}
        method = resolved_cfg.get("method", "GET").upper()
        # Si des stations sont fournies, les ajouter à la config pour le slicing
        if stations_data:
            slicer_cfg = resolved_cfg.get("slicer", {})
            if slicer_cfg.get("mode") == "station_month" and slicer_cfg.get("stations_source") == "dagster_asset":
                slicer_cfg["stations"] = stations_data
                resolved_cfg["slicer"] = slicer_cfg
        
        slices: Deque[Slice] = deque(build_slices(resolved_cfg))
        
        # Statistiques globales
        total_slices = len(slices)
        total_records_processed = 0
        total_requests_made = 0
        start_time = time.time()
        
        log = dagster_log.info if dagster_log else print

        log(f"🚀 DLT: Démarrage ingestion {validated_cfg.name} - {total_slices} slices à traiter")
        log(f"📊 DLT: Configuration: {resolved_cfg.get('base_url', '')}{resolved_cfg.get('path', '')}")
        log(f"🔑 DLT: Clés primaires: {validated_cfg.primary_keys}")

        # Afficher les détails des slices générés
        log(f"📋 DLT: Slices générés ({total_slices}):")
        for i, slice_obj in enumerate(slices):
            slice_info = f"   Slice {i+1}: {slice_obj.scope} - {slice_obj.params}"
            log(f"📋 DLT: {slice_info}")
            if i >= 4:  # Limiter l'affichage
                remaining = len(slices) - 5
                if remaining > 0:
                    remaining_info = f"   ... et {remaining} autres slices"
                    log(f"📋 DLT: {remaining_info}")
                break

        try:
            slice_count = 0
            while slices:
                slice_obj = slices.popleft()
                slice_count += 1
                page = 1
                cursor = None  # Pour pagination par cursor
                buffered_batches: List[List[Dict[str, Any]]] = []
                slice_records = 0
                slice_requests = 0
                truncated = False
                
                slice_msg = f"📦 Traitement slice {slice_count}/{total_slices}: {slice_obj.scope} - {slice_obj.params}"
                log(f"📦 DLT: {slice_msg}")

                while True:
                    # Support pagination par cursor ou par page
                    if pagination and pagination.get("type") == "cursor":
                        params = _build_params(resolved_cfg, slice_obj, cursor_value=cursor)
                    else:
                        params = _build_params(resolved_cfg, slice_obj, page if pagination else None)
                    request_kwargs: Dict[str, Any] = {"params": params}
                    if method in {"POST", "PUT", "PATCH"}:
                        request_kwargs["json_body"] = params
                    
                    # Log de la requête
                    log(f"🌐 Requête {slice_requests + 1}: {method} {resolved_cfg['path']} avec params: {params}")
                    
                    request_start = time.time()
                    payload = http_client.request(method, resolved_cfg["path"], **request_kwargs)
                    request_duration = time.time() - request_start
                    slice_requests += 1
                    total_requests_made += 1
                    
                    batch = list(http_client.extract_records(payload, resolved_cfg.get("records_path")))

                    # Nettoyer immédiatement les champs critiques pour éviter toute valeur NULL
                    # d'atteindre les étapes ultérieures (ex: troncature sans retraitement).
                    for record in batch:
                        _clean_critical_fields(record, resolved_cfg)

                    buffered_batches.append(batch)
                    slice_records += len(batch)
                    
                    req_msg = f"✅ Requête {slice_requests} réussie: {len(batch)} records en {request_duration:.2f}s"
                    log(f"✅ DLT: {req_msg}")

                    if needs_truncation(slice_records, resolved_cfg):
                        truncated = True
                        fallbacks = resolved_cfg.get('fallbacks') or {}
                        threshold = fallbacks.get('truncation_threshold', 'non définie')
                        log(f"⚠️ Troncature détectée: {slice_records} records (limite: {threshold})")
                        break

                    if _should_stop_pagination(payload, batch, pagination):
                        log(f"🛑 Arrêt pagination: condition remplie (batch de {len(batch)} records)")
                        break

                    # Mise à jour du cursor ou de la page
                    if pagination and pagination.get("type") == "cursor":
                        # Extraire le cursor du lien next
                        cursor_path = pagination.get("cursor_path", "$.next")
                        import jsonpath_ng
                        jsonpath_expr = jsonpath_ng.parse(cursor_path)
                        matches = [match.value for match in jsonpath_expr.find(payload)]
                        if matches and matches[0]:
                            # Extraire le cursor de l'URL
                            from urllib.parse import urlparse, parse_qs
                            next_url = matches[0]
                            parsed = urlparse(next_url)
                            cursor_param = pagination.get("cursor_param", "cursor")
                            query_params = parse_qs(parsed.query)
                            cursor = query_params.get(cursor_param, [None])[0]
                            if not cursor:
                                break  # Pas de cursor valide, arrêt
                        else:
                            break  # Pas de next, arrêt pagination
                    else:
                        page += 1

                if truncated:
                    fallback_slices = generate_fallback_slices(slice_obj, resolved_cfg, slice_obj.level)
                    if not fallback_slices:
                        log(f"⚠️ Aucun fallback disponible pour slice {slice_obj.slice_id}")
                        for batch in buffered_batches:
                            for record in batch:
                                record["_slice_id"] = slice_obj.slice_id
                                record["_scope"] = slice_obj.scope
                                yield record
                        continue
                    
                    log(f"🔄 Génération de {len(fallback_slices)} slices de fallback")
                    for new_slice in reversed(fallback_slices):
                        slices.appendleft(new_slice)
                    continue

                # Traitement des records
                for batch in buffered_batches:
                    for record in batch:
                        record["_slice_id"] = slice_obj.slice_id
                        record["_scope"] = slice_obj.scope
                        yield record
                
                total_records_processed += slice_records
                log(f"✅ Slice {slice_count}/{total_slices} terminé: {slice_records} records en {slice_requests} requêtes")
                
                # Log de progression toutes les 5 slices
                if slice_count % 5 == 0:
                    elapsed = time.time() - start_time
                    avg_time_per_slice = elapsed / slice_count
                    remaining_slices = total_slices - slice_count
                    estimated_remaining = remaining_slices * avg_time_per_slice
                    log(f"📈 Progression: {slice_count}/{total_slices} slices ({slice_count/total_slices*100:.1f}%) - "
                              f"Temps écoulé: {elapsed:.1f}s - Temps restant estimé: {estimated_remaining:.1f}s")

        finally:
            if owns_client:
                http_client.close()
            
            # Log final des statistiques
            total_time = time.time() - start_time
            log(f"🎉 Ingestion {validated_cfg.name} terminée!")
            log(f"📊 Statistiques finales:")
            log(f"   • Slices traités: {slice_count}/{total_slices}")
            log(f"   • Total records: {total_records_processed}")
            log(f"   • Total requêtes: {total_requests_made}")
            log(f"   • Temps total: {total_time:.2f}s")
            if total_time > 0:
                log(f"   • Records/seconde: {total_records_processed/total_time:.2f}")
                log(f"   • Requêtes/seconde: {total_requests_made/total_time:.2f}")

    return stream


def run_pipeline(
    cfg: Dict[str, Any],
    *,
    bucket_url: Optional[str] = None,
    credentials: Optional[Dict[str, Any]] = None,
    dataset_name: Optional[str] = None,
    file_format: str = "json",
    layout: Optional[str] = None,
    state_fs_options: Optional[Dict[str, Any]] = None,
    dagster_log: Optional[logging.Logger] = None,
    stations_data: Optional[list[str]] = None,
    partition_date: Optional[str] = None,
) -> dlt.LoadInfo:
    validated_cfg = validate_config(cfg)
    resolved_cfg = validated_cfg.model_dump(mode="python")

    destination_kwargs: Dict[str, Any] = {}
    resolved_bucket = bucket_url or resolved_cfg.get("bucket_url")
    
    # Configuration MinIO par défaut si pas de bucket spécifié
    if not resolved_bucket:
        minio_endpoint = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
        minio_bucket = os.getenv("MINIO_BRONZE_BUCKET", "bronze")
        resolved_bucket = f"s3://{minio_bucket}"
        
        # Configuration des credentials MinIO
        destination_kwargs["credentials"] = {
            "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "endpoint_url": minio_endpoint,
            "region_name": os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        }
    
    if resolved_bucket:
        destination_kwargs["bucket_url"] = resolved_bucket
    if credentials:
        destination_kwargs["credentials"] = credentials

    explicit_file_format = resolved_cfg.get("file_format") or file_format
    if explicit_file_format:
        destination_kwargs["file_format"] = explicit_file_format
    if layout or resolved_cfg.get("layout"):
        destination_kwargs["layout"] = resolved_cfg.get("layout") or layout

    # Remove empty values to let dlt rely on defaults when not provided
    destination_kwargs = {k: v for k, v in destination_kwargs.items() if v}

    target_dataset = dataset_name or resolved_cfg.get("dataset_name") or "bronze"
    destination_kwargs.setdefault("layout", "{table_name}/{curr_date}/data.json")

    destination = filesystem(**destination_kwargs) if destination_kwargs else filesystem()

    pipeline_name = f"hubeau_{resolved_cfg['name']}"
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=target_dataset,
    )

    if dagster_log:
        dagster_log.info(f"🏃 Démarrage pipeline DLT: {pipeline_name}")
        dagster_log.info(f"🎯 Destination: {destination}")
        dagster_log.info(f"📁 Dataset: {target_dataset}")
    
    pipeline_start_time = time.time()
    
    # Remplacer les templates dans la configuration
    if partition_date:
        resolved_cfg = _replace_templates(resolved_cfg, partition_date)
    
    with HttpClient(resolved_cfg) as http_client:
        load_info = pipeline.run(hubeau_source(resolved_cfg, client=http_client, dagster_log=dagster_log, stations_data=stations_data))

    pipeline_duration = time.time() - pipeline_start_time
    
    # Extraction des statistiques détaillées
    total_rows = 0
    total_files = 0
    load_packages_info = []
    
    if hasattr(load_info, 'load_packages') and load_info.load_packages:
        for package in load_info.load_packages:
            package_info = {
                'load_id': package.load_id,
                'jobs': []
            }
            
            if hasattr(package, 'jobs'):
                for job in package.jobs:
                    # Handle both string and object job types
                    if isinstance(job, str):
                        job_info = {
                            'job_id': job,
                            'job_file_type': 'unknown',
                            'records_count': 0,
                            'file_size': 0
                        }
                    else:
                        job_info = {
                            'job_id': getattr(job, 'job_id', str(job)),
                            'job_file_type': getattr(job, 'job_file_type', 'unknown'),
                            'records_count': getattr(job, 'records_count', 0),
                            'file_size': getattr(job, 'file_size', 0)
                        }
                    package_info['jobs'].append(job_info)
                    
                    if job_info['job_file_type'] == "data":
                        total_rows += job_info['records_count']
                        total_files += 1
    
    if dagster_log:
        dagster_log.info(f"✅ Pipeline DLT terminé en {pipeline_duration:.2f}s")
        dagster_log.info(f"📊 Résultats:")
        dagster_log.info(f"   • Packages chargés: {len(load_info.load_packages) if hasattr(load_info, 'load_packages') else 0}")
        dagster_log.info(f"   • Fichiers de données: {total_files}")
        dagster_log.info(f"   • Total lignes: {total_rows:,}")
        if pipeline_duration > 0:
            dagster_log.info(f"   • Vitesse: {total_rows/pipeline_duration:.0f} lignes/seconde")

    state = pipeline.state
    if state:
        save_state_copy(
            resolved_cfg["source"],
            resolved_cfg["name"],
            state,
            fs_url=resolved_cfg.get("state_store"),
            fs_options=_normalise_state_options(state_fs_options),
        )
        if dagster_log:
            dagster_log.info(f"💾 État sauvegardé")

    return load_info


def _clean_critical_fields(record: Dict[str, Any], cfg: Dict[str, Any]) -> None:
    """Clean up NULL values in critical fields for all Hub'Eau APIs."""
    
    # Get primary keys from configuration
    primary_keys = cfg.get("primary_keys", [])
    api_name = cfg.get("name", "")
    
    # Strategy 1: Direct field substitution (field1 -> field2) - PAR API
    field_substitutions = {}
    
    if "piezometry" in api_name or "piezometrie" in api_name:
        field_substitutions = {
            "code_bss": ["bss_id", "code_station"],
        }
    elif "ecoulement" in api_name or "onde" in api_name:
        field_substitutions = {
            "code_station": ["code_station_hydrobio", "code_site"],
        }
    elif "hydrobio" in api_name:
        field_substitutions = {
            "code_station_hydrobio": ["code_station"],
            "code_indice": ["id_indice"],
            "id_taxon": ["code_taxon"],
        }
    elif "hydrometry" in api_name or "hydrometrie" in api_name:
        field_substitutions = {
            "code_station": ["code_site"],
        }
    elif "quality" in api_name or "qualite" in api_name:
        field_substitutions = {
            "code_bss": ["bss_id", "code_ouvrage"],
            "libelle_parametre": ["nom_parametre"],
            "code_unite": ["unite"],
        }
    elif "temperature" in api_name:
        field_substitutions = {
            "code_station": ["code_site", "code_station_hydrobio"],
        }
    elif "prelevement" in api_name:
        field_substitutions = {
            "code_ouvrage": ["code_bss", "code_site"],
        }
    
    # Strategy 2: Generate unique identifiers when all fields are NULL
    timestamp_fields = ["timestamp_mesure", "date_mesure", "date_observation", "date_prelevement", "date_obs"]
    
    for primary_key in primary_keys:
        if record.get(primary_key) is None:
            # Log pour debug
            print(f"🔍 NULL détecté dans {primary_key} pour API {api_name}")
            print(f"   Champs disponibles: {list(record.keys())[:10]}")
            print(f"   Substitutions possibles: {field_substitutions.get(primary_key, [])}")
            
            # Try substitutions first
            substituted = False
            for substitute_field in field_substitutions.get(primary_key, []):
                if record.get(substitute_field) is not None:
                    record[primary_key] = record[substitute_field]
                    substituted = True
                    print(f"   ✅ Substitué par {substitute_field}: {record[primary_key]}")
                    break
            
            # If no substitution worked, generate a unique identifier
            if not substituted:
                # Try to find a timestamp for uniqueness
                timestamp_value = None
                for ts_field in timestamp_fields:
                    if record.get(ts_field) is not None:
                        timestamp_value = record[ts_field]
                        break
                
                # Generate unique identifier
                if timestamp_value:
                    record[primary_key] = f"unknown_{primary_key}_{timestamp_value}_{api_name}"
                else:
                    # Fallback with current time
                    import time
                    record[primary_key] = f"unknown_{primary_key}_{int(time.time())}_{api_name}"
    
    # Strategy 3: Clean up specific problematic fields
    # Remove None values from critical fields that could cause issues
    critical_fields = ["code_unite", "libelle_parametre", "grandeur_hydro", "code_ecoulement"]
    for field in critical_fields:
        if record.get(field) is None:
            record[field] = "unknown"


def _normalise_state_options(options: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Convert user-provided state options to fsspec compatible values."""

    if not options:
        return None

    normalised: Dict[str, Any] = {}
    client_kwargs: Dict[str, Any] = {}
    for key, value in options.items():
        if value in (None, ""):
            continue
        if key == "aws_access_key_id":
            normalised["key"] = str(value)
        elif key == "aws_secret_access_key":
            normalised["secret"] = str(value)
        elif key in {"endpoint_url", "region_name"}:
            client_kwargs[key] = value
        else:
            normalised[key] = value
    if client_kwargs:
        merged_kwargs = dict(normalised.get("client_kwargs", {}))
        merged_kwargs.update(client_kwargs)
        normalised["client_kwargs"] = merged_kwargs
    return normalised or None


def run_from_file(config_path: str | Path) -> dlt.LoadInfo:
    import yaml

    with open(config_path, "r", encoding="utf-8") as fp:
        cfg = yaml.safe_load(fp)
    return run_pipeline(cfg)


__all__ = ["run_pipeline", "run_from_file", "hubeau_source"]
