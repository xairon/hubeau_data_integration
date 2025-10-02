"""Generic dlt pipeline that ingests HubEau endpoints defined via YAML."""
from __future__ import annotations

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
def hubeau_source(cfg: Dict[str, Any]):
    validated_cfg = validate_config(cfg)
    client = HttpClient(cfg)

    @dlt.resource(
        name=validated_cfg.name,
        write_disposition="append",
        primary_key=validated_cfg.primary_keys,
    )
    def stream() -> Iterator[Dict[str, Any]]:
        pagination = cfg.get("pagination") or {}
        slices: Deque[Slice] = deque(build_slices(cfg))

        while slices:
            slice_obj = slices.popleft()
            page = 1
            buffered_batches: List[List[Dict[str, Any]]] = []
            total_records = 0
            truncated = False

            while True:
                params = _build_params(cfg, slice_obj, page if pagination else None)
                payload = client.get(cfg["path"], params=params)
                batch = list(client.extract_records(payload, cfg.get("records_path")))
                buffered_batches.append(batch)
                total_records += len(batch)

                if needs_truncation(total_records, cfg):
                    truncated = True
                    break

                if _should_stop_pagination(payload, batch, pagination):
                    break

                page += 1

            if truncated:
                fallback_slices = generate_fallback_slices(slice_obj, cfg, slice_obj.level)
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
                    record["_slice_id"] = slice_obj.slice_id
                    record["_scope"] = slice_obj.scope
                    yield record

    return stream


@dlt.pipeline(
    pipeline_name="hubeau_to_minio",
    destination="filesystem",
    dataset_name="bronze",
)
def run_pipeline(
    cfg: Dict[str, Any],
    *,
    bucket_url: Optional[str] = None,
    credentials: Optional[Dict[str, Any]] = None,
    dataset_name: Optional[str] = None,
    file_format: str = "parquet",
    layout: Optional[str] = None,
    state_fs_options: Optional[Dict[str, Any]] = None,
) -> dlt.LoadInfo:
    destination_kwargs: Dict[str, Any] = {
        "bucket_url": bucket_url,
        "credentials": credentials,
        "file_format": file_format,
    }
    if layout:
        destination_kwargs["layout"] = layout
    # Remove empty values to let dlt rely on defaults when not provided
    destination_kwargs = {k: v for k, v in destination_kwargs.items() if v}

    target_dataset = dataset_name or cfg.get("dataset_name") or cfg["source"]
    destination_kwargs.setdefault(
        "layout",
        "{schema_name}/{table_name}/format=parquet/run_date={curr_date}/part-{file_id}",
    )

    destination = filesystem(**destination_kwargs)
    pipeline = run_pipeline.pipeline(destination=destination, dataset_name=target_dataset)
    load_info = pipeline.run(hubeau_source(cfg))
    state = pipeline.state.asdict()
    save_state_copy(
        cfg["source"],
        cfg["name"],
        state,
        fs_url=cfg.get("state_store"),
        fs_options=state_fs_options,
    )
    return load_info


def run_from_file(config_path: str | Path) -> dlt.LoadInfo:
    import yaml

    with open(config_path, "r", encoding="utf-8") as fp:
        cfg = yaml.safe_load(fp)
    return run_pipeline(cfg)


__all__ = ["run_pipeline", "run_from_file", "hubeau_source"]
