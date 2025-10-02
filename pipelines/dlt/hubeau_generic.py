"""Generic dlt pipeline that ingests HubEau endpoints defined via YAML."""
from __future__ import annotations

import os
import re
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, Iterator, List, Optional

import dlt
from jsonpath_ng import parse
from dlt.destinations import filesystem

from .http_client import HttpClient
from .schema import validate_config
from .slicing import Slice, build_slices, generate_fallback_slices, needs_truncation
from .state import save_state_copy


def _build_params(cfg: Dict[str, Any], slice_obj: Slice, page: int | None = None) -> Dict[str, Any]:
    params = dict(cfg.get("params_default", {}))
    params.update(slice_obj.params)
    pagination = cfg.get("pagination") or {}
    if pagination.get("type") == "page" and page is not None:
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
def hubeau_source(cfg: Dict[str, Any], *, client: HttpClient | None = None):
    """Return the dlt resource streaming Hub'Eau data."""

    validated_cfg = validate_config(cfg)
    resolved_cfg = validated_cfg.model_dump(mode="python")

    http_client = client or HttpClient(resolved_cfg)
    owns_client = client is None

    @dlt.resource(
        name=validated_cfg.name,
        write_disposition="append",
        primary_key=validated_cfg.primary_keys,
    )
    def stream() -> Iterator[Dict[str, Any]]:
        pagination = resolved_cfg.get("pagination") or {}
        method = resolved_cfg.get("method", "GET").upper()
        slices: Deque[Slice] = deque(build_slices(resolved_cfg))

        try:
            while slices:
                slice_obj = slices.popleft()
                page = 1
                buffered_batches: List[List[Dict[str, Any]]] = []
                total_records = 0
                truncated = False

                while True:
                    params = _build_params(resolved_cfg, slice_obj, page if pagination else None)
                    request_kwargs: Dict[str, Any] = {"params": params}
                    if method in {"POST", "PUT", "PATCH"}:
                        request_kwargs["json_body"] = params
                    payload = http_client.request(method, resolved_cfg["path"], **request_kwargs)
                    batch = list(http_client.extract_records(payload, resolved_cfg.get("records_path")))
                    buffered_batches.append(batch)
                    total_records += len(batch)

                    if needs_truncation(total_records, resolved_cfg):
                        truncated = True
                        break

                    if _should_stop_pagination(payload, batch, pagination):
                        break

                    page += 1

                if truncated:
                    fallback_slices = generate_fallback_slices(slice_obj, resolved_cfg, slice_obj.level)
                    if not fallback_slices:
                        for batch in buffered_batches:
                            for record in batch:
                                record["_slice_id"] = slice_obj.slice_id
                                record["_scope"] = slice_obj.scope
                                yield record
                        continue
                    for new_slice in reversed(fallback_slices):
                        slices.appendleft(new_slice)
                    continue

                for batch in buffered_batches:
                    for record in batch:
                        # Clean up NULL values in critical fields for all APIs
                        _clean_critical_fields(record, resolved_cfg)
                        
                        record["_slice_id"] = slice_obj.slice_id
                        record["_scope"] = slice_obj.scope
                        yield record
        finally:
            if owns_client:
                http_client.close()

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
) -> dlt.LoadInfo:
    validated_cfg = validate_config(cfg)
    resolved_cfg = validated_cfg.model_dump(mode="python")

    destination_kwargs: Dict[str, Any] = {}
    resolved_bucket = bucket_url or resolved_cfg.get("bucket_url")
    if not resolved_bucket:
        env_path = os.getenv("DESTINATION__FILESYSTEM__PATH")
        if env_path:
            resolved_bucket = Path(env_path).expanduser().resolve().as_uri()
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

    with HttpClient(resolved_cfg) as http_client:
        load_info = pipeline.run(hubeau_source(resolved_cfg, client=http_client))

    state = pipeline.state
    if state:
        save_state_copy(
            resolved_cfg["source"],
            resolved_cfg["name"],
            state,
            fs_url=resolved_cfg.get("state_store"),
            fs_options=_normalise_state_options(state_fs_options),
        )

    return load_info


def _clean_critical_fields(record: Dict[str, Any], cfg: Dict[str, Any]) -> None:
    """Clean up NULL values in critical fields for all Hub'Eau APIs."""
    
    # Get primary keys from configuration
    primary_keys = cfg.get("primary_keys", [])
    
    # Define field mapping strategies for each API
    api_name = cfg.get("name", "")
    
    # Strategy 1: Direct field substitution (field1 -> field2)
    field_substitutions = {
        # Piézométrie
        "code_bss": ["bss_id", "code_station"],
        
        # Écoulement
        "code_station": ["code_station_hydrobio", "code_site"],
        
        # Hydrobiologie
        "code_station_hydrobio": ["code_station"],
        "code_indice": ["id_indice"],
        "id_taxon": ["code_taxon"],
        
        # Hydrométrie
        "code_station": ["code_site", "code_station_hydrobio"],
        
        # Qualité
        "code_bss": ["bss_id", "code_ouvrage"],
        "libelle_parametre": ["nom_parametre"],
        "code_unite": ["unite"],
        
        # Température
        "code_station": ["code_site", "code_station_hydrobio"],
        
        # Prélèvements
        "code_ouvrage": ["code_bss", "code_site"],
    }
    
    # Strategy 2: Generate unique identifiers when all fields are NULL
    timestamp_fields = ["timestamp_mesure", "date_mesure", "date_observation", "date_prelevement", "date_obs"]
    
    for primary_key in primary_keys:
        if record.get(primary_key) is None:
            # Try substitutions first
            substituted = False
            for substitute_field in field_substitutions.get(primary_key, []):
                if record.get(substitute_field) is not None:
                    record[primary_key] = record[substitute_field]
                    substituted = True
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
