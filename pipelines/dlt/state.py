"""Helpers to persist dlt state snapshots locally and to object storage."""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from fsspec.core import url_to_fs

try:  # pragma: no cover - optional dependency
    import pendulum

    _PENDULUM_TYPES: Iterable[type] = (pendulum.DateTime,)
except ImportError:  # pragma: no cover - optional dependency
    pendulum = None
    _PENDULUM_TYPES = ()


def _local_state_path(source: str, stream: str) -> Path:
    return Path(".state") / f"{source}_{stream}.json"


def _json_serializer(obj):
    """Custom JSON serializer to handle DateTime objects."""
    if hasattr(obj, 'isoformat'):
        # Handle datetime objects
        return obj.isoformat()
    elif hasattr(obj, '__dict__'):
        # Handle other objects with __dict__
        return obj.__dict__
    else:
        # Fallback to string representation
        return str(obj)


def save_state_copy(
    source: str,
    stream: str,
    state: Any,
    *,
    fs_url: str | None = None,
    fs_options: Optional[Dict[str, Any]] = None,
) -> Path:
    path = _local_state_path(source, stream)
    path.parent.mkdir(parents=True, exist_ok=True)
    serializable_state = _make_json_serializable(state)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(serializable_state, fp, ensure_ascii=False, indent=2)

    if fs_url:
        options = dict(fs_options or {})
        options.setdefault("skip_instance_cache", True)
        fs, base_path = url_to_fs(fs_url, **options)
        remote_path = f"{base_path.rstrip('/')}/{source}/{stream}.state.json"
        with fs.open(remote_path, "w", encoding="utf-8") as remote:
            json.dump(serializable_state, remote, ensure_ascii=False)
    return path


def _make_json_serializable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if _PENDULUM_TYPES and isinstance(value, tuple(_PENDULUM_TYPES)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _make_json_serializable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_make_json_serializable(v) for v in value]
    if hasattr(value, "to_dict"):
        return _make_json_serializable(value.to_dict())
    return repr(value)
