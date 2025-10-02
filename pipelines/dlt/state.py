"""Helpers to persist dlt state snapshots locally and to object storage."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

import fsspec


def _local_state_path(source: str, stream: str) -> Path:
    return Path(".state") / f"{source}_{stream}.json"


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
    with path.open("w", encoding="utf-8") as fp:
        json.dump(state, fp, ensure_ascii=False, indent=2)

    if fs_url:
        options = dict(fs_options or {})
        options.setdefault("skip_instance_cache", True)
        fs = fsspec.filesystem("s3", **options)
        remote_path = os.path.join(fs_url.rstrip("/"), f"{source}/{stream}.state.json")
        with fs.open(remote_path, "w") as remote:
            json.dump(state, remote, ensure_ascii=False)
    return path
