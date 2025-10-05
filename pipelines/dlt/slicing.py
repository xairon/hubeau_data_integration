"""Slice generation helpers for the generic dlt pipeline."""
from __future__ import annotations

import calendar
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional

import yaml

TRUNCATION_DEFAULT = float("inf")  # Par défaut, pas de troncature (récupère toutes les données)


@dataclass
class Slice:
    """Represents a logical slice to query from an endpoint."""

    params: Dict[str, Any]
    slice_id: str
    scope: str = "global"
    metadata: Dict[str, Any] = field(default_factory=dict)
    level: int = 0

    def to_dict(self) -> Dict[str, Any]:
        data = {"params": self.params, "slice_id": self.slice_id, "scope": self.scope}
        if self.metadata:
            data["metadata"] = self.metadata
        data["level"] = self.level
        return data


def daterange(start: date, end: date, step_days: int) -> Iterator[tuple[date, date]]:
    current = start
    delta = timedelta(days=step_days)
    while current <= end:
        stop = min(end, current + delta - timedelta(days=1))
        yield current, stop
        current += delta


def month_windows(start: date, end: date) -> Iterator[tuple[date, date]]:
    cursor = date(start.year, start.month, 1)
    final = date(end.year, end.month, calendar.monthrange(end.year, end.month)[1])
    while cursor <= final:
        last_day = calendar.monthrange(cursor.year, cursor.month)[1]
        period_end = date(cursor.year, cursor.month, last_day)
        yield cursor, min(period_end, final)
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)


def load_reference_list(ref_cfg: Dict[str, Any]) -> list[str]:
    path = Path(ref_cfg["path"])
    if not path.exists():
        raise FileNotFoundError(f"Reference file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        content = yaml.safe_load(f)
    if isinstance(content, list):
        return content
    values = content.get(ref_cfg["key"], []) if isinstance(content, dict) else []
    if not isinstance(values, list):
        raise ValueError("Reference file must contain a list")
    return values


def _resolve_reference_values(cfg: Dict[str, Any], key: str) -> list[str]:
    slicer_cfg = cfg.get("slicer", {})
    if key in slicer_cfg:
        values = slicer_cfg[key]
        if isinstance(values, list):
            return values

    pre_scan_cfg = (cfg.get("pre_scan") or {}).get(key.rstrip("s") + "s", {})
    if pre_scan_cfg.get("enabled") and "values" in pre_scan_cfg:
        values = pre_scan_cfg["values"]
        if isinstance(values, list):
            return values
    if pre_scan_cfg.get("enabled") and "path" in pre_scan_cfg:
        file_path = Path(pre_scan_cfg["path"])
        if not file_path.exists():
            raise FileNotFoundError(f"Pre-scan file not found: {file_path}")
        if file_path.suffix in {".yml", ".yaml"}:
            with file_path.open("r", encoding="utf-8") as fp:
                loaded = yaml.safe_load(fp)
            values = loaded if isinstance(loaded, list) else loaded.get(pre_scan_cfg.get("key", "values"), [])
            if not isinstance(values, list):
                raise ValueError("Pre-scan YAML must contain a list of values")
            return values
        # Fallback: assume text file with one value per line
        return [line.strip() for line in file_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if slicer_cfg.get("reference"):
        return load_reference_list(slicer_cfg["reference"])
    return []


def build_slices(
    cfg: Dict[str, Any], *, override_start: Optional[str] = None, override_end: Optional[str] = None
) -> Iterable[Slice]:
    slicer_cfg = cfg.get("slicer", {})
    mode = slicer_cfg.get("mode", "global")  # Par défaut, mode global (une seule slice)

    if mode == "datetime":
        start_date = override_start or slicer_cfg.get("start_date")
        end_offset_days = slicer_cfg.get("end_offset_days", 1)
        if not start_date:
            raise ValueError("datetime slicer requires start_date")
        start = date.fromisoformat(start_date)
        today = date.today()
        end_candidate = today - timedelta(days=end_offset_days)
        if override_end:
            end_candidate = min(end_candidate, date.fromisoformat(override_end))
        window_days = slicer_cfg.get("window_days", 1)
        start_param = slicer_cfg["start_param"]
        end_param = slicer_cfg["end_param"]
        for d0, d1 in daterange(start, end_candidate, window_days):
            params = {start_param: d0.isoformat(), end_param: d1.isoformat()}
            metadata = {
                "mode": "datetime",
                "start": d0.isoformat(),
                "end": d1.isoformat(),
                "start_param": start_param,
                "end_param": end_param,
            }
            yield Slice(params=params, slice_id=f"{d0.isoformat()}_{d1.isoformat()}", metadata=metadata)
    elif mode == "global":
        # Mode global : une seule slice sans paramètres de filtrage
        yield Slice(params={}, slice_id="global", scope="global", metadata={"mode": "global"})
    elif mode == "dept":
        departments = slicer_cfg.get("values")
        if not departments:
            departments = _resolve_reference_values(cfg, "departments")
        if not departments:
            raise ValueError("dept slicer requires either values or reference")
        param_name = slicer_cfg.get("param", "code_departement")
        for dept in departments:
            params = {param_name: dept}
            metadata = {"mode": "dept", "dept": dept, "param": param_name}
            yield Slice(params=params, slice_id=f"dept_{dept}", scope=f"dept-{dept}", metadata=metadata)
    elif mode == "dept_datetime":
        # Mode dept_datetime : chunking par département + temps
        dept_list = slicer_cfg.get("dept_list")
        if not dept_list:
            raise ValueError("dept_datetime slicer requires dept_list")
        
        dept_chunk_size = slicer_cfg.get("dept_chunk_size", 5)
        dept_param = slicer_cfg.get("dept_param", "code_departement")
        
        # Paramètres temporels
        start_date = override_start or slicer_cfg.get("start_date")
        end_offset_days = slicer_cfg.get("end_offset_days", 1)
        if not start_date:
            raise ValueError("dept_datetime slicer requires start_date")
        start = date.fromisoformat(start_date)
        today = date.today()
        end_candidate = today - timedelta(days=end_offset_days)
        if override_end:
            end_candidate = min(end_candidate, date.fromisoformat(override_end))
        window_days = slicer_cfg.get("window_days", 30)
        start_param = slicer_cfg["start_param"]
        end_param = slicer_cfg["end_param"]
        
        # Chunking par département
        for i in range(0, len(dept_list), dept_chunk_size):
            dept_chunk = dept_list[i:i + dept_chunk_size]
            
            # Chunking temporel - Utiliser month_windows pour des fenêtres mensuelles exactes
            # Si window_days est spécifié et > 30, utiliser daterange pour des fenêtres personnalisées
            # Sinon, utiliser month_windows pour des fenêtres mensuelles exactes
            if window_days and window_days > 30:
                # Fenêtres personnalisées (ex: trimestrielles avec window_days=90)
                for d0, d1 in daterange(start, end_candidate, window_days):
                    params = {
                        dept_param: ",".join(dept_chunk),
                        start_param: d0.isoformat(),
                        end_param: d1.isoformat(),
                    }
                    metadata = {
                        "mode": "dept_datetime",
                        "dept_chunk": dept_chunk,
                        "start": d0.isoformat(),
                        "end": d1.isoformat(),
                        "dept_param": dept_param,
                        "start_param": start_param,
                        "end_param": end_param,
                        "window_type": "custom"
                    }
                    dept_str = "_".join(dept_chunk)
                    yield Slice(
                        params=params,
                        slice_id=f"dept_{dept_str}_{d0.isoformat()}",
                        scope=f"dept-{dept_str}",
                        metadata=metadata,
                    )
            else:
                # Fenêtres mensuelles exactes (recommandé pour la plupart des cas)
                for d0, d1 in month_windows(start, end_candidate):
                    params = {
                        dept_param: ",".join(dept_chunk),
                        start_param: d0.isoformat(),
                        end_param: d1.isoformat(),
                    }
                    metadata = {
                        "mode": "dept_datetime",
                        "dept_chunk": dept_chunk,
                        "start": d0.isoformat(),
                        "end": d1.isoformat(),
                        "dept_param": dept_param,
                        "start_param": start_param,
                        "end_param": end_param,
                        "window_type": "monthly"
                    }
                    dept_str = "_".join(dept_chunk)
                    yield Slice(
                        params=params,
                        slice_id=f"dept_{dept_str}_{d0.isoformat()}",
                        scope=f"dept-{dept_str}",
                        metadata=metadata,
                    )
    elif mode == "station_month":
        stations = _resolve_reference_values(cfg, "stations")
        if not stations:
            raise ValueError("station_month slicer requires stations/reference")

        # Autoriser les alias historiques et valeurs dynamiques (ex: stations injectées via Dagster)
        window_days = slicer_cfg.get("window_days", 30)
        if not isinstance(window_days, int) or window_days <= 0:
            window_days = 30

        start_date_raw = slicer_cfg.get("start_date")
        if start_date_raw:
            if isinstance(start_date_raw, date):
                start_date = start_date_raw
            else:
                try:
                    start_date = date.fromisoformat(str(start_date_raw))
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        "station_month slicer start_date must be ISO formatted (YYYY-MM-DD)"
                    ) from exc
        else:
            # Fallback intelligent : on se limite à la fenêtre récente (ex: API temps réel)
            start_date = date.today() - timedelta(days=window_days)

        end_offset_days = slicer_cfg.get("end_offset_days", 1)
        if not isinstance(end_offset_days, int) or end_offset_days < 0:
            end_offset_days = 1
        end_candidate = date.today() - timedelta(days=end_offset_days)
        if start_date > end_candidate:
            start_date = end_candidate

        # Normaliser la fenêtre calculée pour les logs, les fallbacks et les tests
        slicer_cfg["start_date"] = start_date.isoformat()
        slicer_cfg["end_date"] = end_candidate.isoformat()

        station_param = (
            slicer_cfg.get("station_param")
            or slicer_cfg.get("param")
            or "code_station"
        )
        start_param = slicer_cfg.get("start_param", "date_debut")
        end_param = slicer_cfg.get("end_param", "date_fin")
        for station in stations:
            for d0, d1 in month_windows(start_date, end_candidate):
                params = {
                    station_param: station,
                    start_param: d0.isoformat(),
                    end_param: d1.isoformat(),
                }
                metadata = {
                    "mode": "station_month",
                    "station": station,
                    "start": d0.isoformat(),
                    "end": d1.isoformat(),
                    "station_param": station_param,
                    "start_param": start_param,
                    "end_param": end_param,
                }
                yield Slice(
                    params=params,
                    slice_id=f"{station}_{d0.isoformat()}",
                    scope=f"station-{station}",
                    metadata=metadata,
                )
    elif mode == "campaign":
        campaigns = slicer_cfg.get("campaigns")
        if not campaigns:
            campaigns = _resolve_reference_values(cfg, "campaigns")
        if not campaigns:
            raise ValueError("campaign slicer requires campaigns/reference")
        for campaign in campaigns:
            slice_id = str(campaign.get("id", campaign)) if isinstance(campaign, dict) else str(campaign)
            metadata = {"mode": "campaign", "campaign": campaign}
            params = campaign if isinstance(campaign, dict) else {"campaign": campaign}
            yield Slice(params=params, slice_id=f"campaign_{slice_id}", scope="campaign", metadata=metadata)
    else:
        raise ValueError(f"Unsupported slicer mode: {mode}")


def _split_slice_day(slice_obj: Slice, cfg: Dict[str, Any], next_level: int) -> list[Slice]:
    meta = slice_obj.metadata
    if not meta or "start" not in meta or "end" not in meta:
        return []
    start = date.fromisoformat(meta["start"])
    end = date.fromisoformat(meta["end"])
    start_param = meta.get("start_param") or cfg["slicer"]["start_param"]
    end_param = meta.get("end_param") or cfg["slicer"]["end_param"]
    result: list[Slice] = []
    for d0, d1 in daterange(start, end, 1):
        params = dict(slice_obj.params)
        params[start_param] = d0.isoformat()
        params[end_param] = d1.isoformat()
        metadata = dict(meta)
        metadata.update({"start": d0.isoformat(), "end": d1.isoformat()})
        result.append(
            Slice(
                params=params,
                slice_id=f"{slice_obj.slice_id}__day_{d0.isoformat()}",
                scope=slice_obj.scope,
                metadata=metadata,
                level=next_level,
            )
        )
    return result


def _split_slice_station_month(slice_obj: Slice, cfg: Dict[str, Any], next_level: int) -> list[Slice]:
    meta = slice_obj.metadata
    if not meta or "start" not in meta or "end" not in meta:
        return []
    stations = _resolve_reference_values(cfg, "stations")
    if not stations:
        raise ValueError("station_month fallback requires station list from pre_scan or slicer config")
    start = date.fromisoformat(meta["start"])
    end = date.fromisoformat(meta["end"])
    start_param = meta.get("start_param") or cfg["slicer"].get("start_param", "date_debut")
    end_param = meta.get("end_param") or cfg["slicer"].get("end_param", "date_fin")
    station_param = cfg["slicer"].get("station_param", "code_station")
    result: list[Slice] = []
    for station in stations:
        for d0, d1 in month_windows(start, end):
            params = dict(slice_obj.params)
            params[station_param] = station
            params[start_param] = d0.isoformat()
            params[end_param] = d1.isoformat()
            metadata = {
                "mode": "station_month",
                "station": station,
                "start": d0.isoformat(),
                "end": d1.isoformat(),
                "start_param": start_param,
                "end_param": end_param,
                "station_param": station_param,
            }
            result.append(
                Slice(
                    params=params,
                    slice_id=f"{slice_obj.slice_id}__station_{station}_{d0.isoformat()}",
                    scope=f"station-{station}",
                    metadata=metadata,
                    level=next_level,
                )
            )
    return result


def generate_fallback_slices(slice_obj: Slice, cfg: Dict[str, Any], current_level: int) -> list[Slice]:
    """Generate fallback slices following the configured split chain."""

    chain = (cfg.get("fallbacks") or {}).get("split_chain", []) or []
    if current_level >= len(chain):
        return []
    strategy = chain[current_level]
    if strategy == "day":
        return _split_slice_day(slice_obj, cfg, current_level + 1)
    if strategy == "station_month":
        return _split_slice_station_month(slice_obj, cfg, current_level + 1)
    raise ValueError(f"Unsupported fallback strategy: {strategy}")


def needs_truncation(count: int, cfg: Dict[str, Any]) -> bool:
    fallbacks_cfg = cfg.get("fallbacks") or {}
    threshold = fallbacks_cfg.get("truncation_threshold", TRUNCATION_DEFAULT)
    return count >= threshold


__all__ = [
    "Slice",
    "build_slices",
    "daterange",
    "generate_fallback_slices",
    "month_windows",
    "needs_truncation",
]
