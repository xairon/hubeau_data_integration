"""Slice generation helpers for the generic dlt pipeline."""
from __future__ import annotations

import calendar
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

import yaml

# Import factorized date utilities
try:
    from hubeau.utils.date_utils import limit_to_partition_year
except ImportError:
    # Fallback if module not yet available (during migration)
    def limit_to_partition_year(start: date, end_candidate: date) -> date:
        if start.month == 1 and start.day == 1:
            end_of_year = date(start.year, 12, 31)
            return min(end_candidate, end_of_year)
        return end_candidate

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


def _resolve_reference_values(cfg: Dict[str, Any], key: str):
    """
    Résout les valeurs de référence depuis la config.

    Returns:
        Union[list[str], Dict[str, List[str]]]: Liste simple ou dict {code: [mois]}
    """
    slicer_cfg = cfg.get("slicer", {})
    if key in slicer_cfg:
        values = slicer_cfg[key]
        # Supporter à la fois list et dict
        if isinstance(values, (list, dict)):
            return values

    pre_scan_cfg = (cfg.get("pre_scan") or {}).get(key.rstrip("s") + "s", {})
    if pre_scan_cfg.get("enabled") and "values" in pre_scan_cfg:
        values = pre_scan_cfg["values"]
        if isinstance(values, (list, dict)):
            return values
    if pre_scan_cfg.get("enabled") and "path" in pre_scan_cfg:
        file_path = Path(pre_scan_cfg["path"])
        if not file_path.exists():
            raise FileNotFoundError(f"Pre-scan file not found: {file_path}")
        if file_path.suffix in {".yml", ".yaml"}:
            with file_path.open("r", encoding="utf-8") as fp:
                loaded = yaml.safe_load(fp)
            values = loaded if isinstance(loaded, list) else loaded.get(pre_scan_cfg.get("key", "values"), [])
            if not isinstance(values, (list, dict)):
                raise ValueError("Pre-scan YAML must contain a list or dict of values")
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

        # ✅ CORRECTION: Pour partitions annuelles, limiter à la fin de l'année de partition
        # Si start_date est "2024-01-01", on limite à "2024-12-31" (pas au-delà)
        end_candidate = limit_to_partition_year(start, end_candidate)

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

        # Vérifier si un temporal_filter est configuré pour ajouter les métadonnées temporelles
        temporal_filter = cfg.get("temporal_filter")
        start_date_str = None
        end_date_str = None
        start_param = None
        end_param = None

        if temporal_filter:
            start_param = temporal_filter.get("start_param")
            end_param = temporal_filter.get("end_param")
            start_date_str = temporal_filter.get("start_date")
            end_offset_days = temporal_filter.get("end_offset_days", 1)

            if start_date_str:
                # La date peut déjà être une date ou une string
                if isinstance(start_date_str, date):
                    start_date_str = start_date_str.isoformat()
                # Calculer la date de fin
                today = date.today()
                end_date = today - timedelta(days=end_offset_days)
                end_date_str = end_date.isoformat()

        for dept in departments:
            params = {param_name: dept}
            metadata = {"mode": "dept", "dept": dept, "param": param_name}

            # Ajouter les métadonnées temporelles si disponibles (pour permettre les fallbacks temporels)
            if start_date_str and end_date_str and start_param and end_param:
                metadata["start"] = start_date_str
                metadata["end"] = end_date_str
                metadata["start_param"] = start_param
                metadata["end_param"] = end_param

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

        # ✅ CORRECTION: Pour partitions annuelles, limiter à la fin de l'année de partition
        # Si start_date est "2024-01-01", on limite à "2024-12-31" (pas au-delà)
        end_candidate = limit_to_partition_year(start, end_candidate)

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

        # ✅ CORRECTION: Pour partitions annuelles, limiter à la fin de l'année de partition
        # Si start_date est "2024-01-01", on limite à "2024-12-31" (pas au-delà)
        if start_date.month == 1 and start_date.day == 1:
            # Partition annuelle détectée (ex: "2024-01-01")
            end_of_year = date(start_date.year, 12, 31)
            end_candidate = min(end_candidate, end_of_year)

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

        # ✅ Support dict ou list pour rétro-compatibilité
        if isinstance(stations, dict):
            stations_months = stations
            station_codes = list(stations.keys())
        else:
            station_codes = stations
            # Mode legacy : générer tous les mois
            all_months = []
            for d0, d1 in month_windows(start_date, end_candidate):
                all_months.append(d0.strftime('%Y-%m'))
            stations_months = {s: all_months for s in stations}

        for station in station_codes:
            # Obtenir les mois actifs pour cette station
            active_months = stations_months.get(station, [])

            for month_str in active_months:
                # Convertir "YYYY-MM" en dates
                year, month = map(int, month_str.split('-'))
                d0 = date(year, month, 1)
                last_day = calendar.monthrange(year, month)[1]
                d1 = date(year, month, last_day)

                # ✅ CORRECTION CRITIQUE: Vérifier que le mois est dans la plage de partition
                # Skip les mois au-delà de end_candidate (ex: 2025 pour partition 2024)
                if d0 > end_candidate or d1 < start_date:
                    continue

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
    elif mode == "station_month_chunked":
        # Mode station_month_chunked : chunke les stations + fenêtres mensuelles INTELLIGENTES
        # ✅ OPTIMISATION: Ne génère que les slices pour les mois où les stations ont réellement des données
        stations = _resolve_reference_values(cfg, "stations")
        if not stations:
            # ✅ Gérer gracieusement le cas où aucune station n'est disponible
            # Au lieu de crasher, retourner une liste vide de slices
            import logging
            logger = logging.getLogger(__name__)
            resource_name = cfg.get('resource', {}).get('name', 'unknown')
            logger.warning(f"No stations provided for {resource_name} with station_month_chunked mode")
            logger.warning("This could indicate no active stations for this period or a configuration issue")
            # Return empty list - pipeline will complete without errors but no data
            return []

        station_chunk_size = slicer_cfg.get("station_chunk_size", 10)
        station_param = slicer_cfg.get("station_param", "code_station")

        # DEBUG LOG pour vérifier le chunk_size utilisé
        import logging
        logger = logging.getLogger(__name__)
        resource_name = cfg.get('resource', {}).get('name', 'unknown')
        logger.warning(f"DEBUG: {resource_name} using station_chunk_size={station_chunk_size}")

        # Paramètres temporels
        start_date = override_start or slicer_cfg.get("start_date")
        end_offset_days = slicer_cfg.get("end_offset_days", 1)
        if not start_date:
            raise ValueError("station_month_chunked slicer requires start_date")
        start = date.fromisoformat(start_date)
        today = date.today()
        end_candidate = today - timedelta(days=end_offset_days)

        # ✅ CORRECTION: Pour partitions annuelles, limiter à la fin de l'année de partition
        if start.month == 1 and start.day == 1:
            end_of_year = date(start.year, 12, 31)
            end_candidate = min(end_candidate, end_of_year)

        if override_end:
            end_candidate = min(end_candidate, date.fromisoformat(override_end))
        start_param = slicer_cfg["start_param"]
        end_param = slicer_cfg["end_param"]

        # ✅ Détecter si stations est un dict {station: [mois]} ou une liste simple
        if isinstance(stations, dict):
            # Mode intelligent : filtrage temporel par station
            stations_months = stations
            station_codes = list(stations.keys())
        else:
            # Mode legacy : liste simple (générer tous les mois)
            station_codes = stations
            # Générer tous les mois possibles pour rétro-compatibilité
            all_months = []
            for d0, d1 in month_windows(start, end_candidate):
                all_months.append(d0.strftime('%Y-%m'))
            stations_months = {s: all_months for s in stations}

        # Chunker les stations
        for i in range(0, len(station_codes), station_chunk_size):
            station_chunk = station_codes[i:i + station_chunk_size]

            # ✅ OPTIMISATION: Calculer l'union des mois disponibles pour ce chunk
            chunk_months = set()
            for station in station_chunk:
                chunk_months.update(stations_months.get(station, []))

            # Pour chaque mois où au moins une station du chunk a des données
            for month_str in sorted(chunk_months):
                # Convertir "YYYY-MM" en dates
                year, month = map(int, month_str.split('-'))
                d0 = date(year, month, 1)
                last_day = calendar.monthrange(year, month)[1]
                d1 = date(year, month, last_day)

                # ✅ CORRECTION CRITIQUE: Vérifier que le mois est dans la plage de partition
                # Skip les mois au-delà de end_candidate (ex: 2025 pour partition 2024)
                if d0 > end_candidate or d1 < start:
                    continue

                # Filtrer les stations du chunk qui ont des données ce mois-ci
                stations_for_month = [s for s in station_chunk if month_str in stations_months.get(s, [])]

                # Ne créer la slice que si au moins une station du chunk a des données ce mois
                if stations_for_month:
                    params = {
                        station_param: ",".join(station_chunk),  # Garder tout le chunk pour requête API
                        start_param: d0.isoformat(),
                        end_param: d1.isoformat(),
                    }
                    metadata = {
                        "mode": "station_month_chunked",
                        "station_chunk": station_chunk,
                        "stations_with_data": stations_for_month,  # Pour debug
                        "start": d0.isoformat(),
                        "end": d1.isoformat(),
                        "station_param": station_param,
                        "start_param": start_param,
                        "end_param": end_param,
                    }
                    station_str = "_".join(station_chunk[:3])  # Limiter taille ID
                    if len(station_chunk) > 3:
                        station_str += f"_and_{len(station_chunk)-3}_more"
                    yield Slice(
                        params=params,
                        slice_id=f"stations_{station_str}_{d0.isoformat()}",
                        scope=f"stations-{len(station_chunk)}",
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


def _split_slice_month(slice_obj: Slice, cfg: Dict[str, Any], next_level: int) -> list[Slice]:
    """Split a slice into monthly sub-slices."""
    meta = slice_obj.metadata
    if not meta or "start" not in meta or "end" not in meta:
        return []
    start = date.fromisoformat(meta["start"])
    end = date.fromisoformat(meta["end"])
    start_param = meta.get("start_param") or cfg["slicer"].get("start_param", "date_debut")
    end_param = meta.get("end_param") or cfg["slicer"].get("end_param", "date_fin")
    result: list[Slice] = []
    for d0, d1 in month_windows(start, end):
        params = dict(slice_obj.params)
        params[start_param] = d0.isoformat()
        params[end_param] = d1.isoformat()
        metadata = dict(meta)
        metadata.update({"start": d0.isoformat(), "end": d1.isoformat()})
        result.append(
            Slice(
                params=params,
                slice_id=f"{slice_obj.slice_id}__month_{d0.isoformat()[:7]}",  # YYYY-MM
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

    # ✅ Support dict ou list
    if isinstance(stations, dict):
        station_codes = list(stations.keys())
    else:
        station_codes = stations

    start = date.fromisoformat(meta["start"])
    end = date.fromisoformat(meta["end"])
    start_param = meta.get("start_param") or cfg["slicer"].get("start_param", "date_debut")
    end_param = meta.get("end_param") or cfg["slicer"].get("end_param", "date_fin")
    station_param = cfg["slicer"].get("station_param", "code_station")
    result: list[Slice] = []
    for station in station_codes:
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
    if strategy == "month":
        return _split_slice_month(slice_obj, cfg, current_level + 1)
    if strategy == "station_month":
        return _split_slice_station_month(slice_obj, cfg, current_level + 1)
    raise ValueError(f"Unsupported fallback strategy: {strategy}")


def needs_truncation(count: int, cfg: Dict[str, Any]) -> bool:
    fallbacks_cfg = cfg.get("fallbacks") or {}
    threshold = fallbacks_cfg.get("truncation_threshold", TRUNCATION_DEFAULT)
    return count >= threshold


def get_active_departments_for_stations(stations_data: list[str], station_type: str) -> list[str]:
    """
    Extrait les départements ayant des stations actives depuis MinIO (plus rapide que l'API).

    Args:
        stations_data: Liste des codes de stations actives
        station_type: Type de stations

    Returns:
        Liste unique et triée des départements ayant des stations
    """
    # Import ici pour éviter dépendances circulaires
    from src.hubeau_pipeline.assets.bronze.dlt_assets import get_active_departments_for_stations as _get_depts

    return _get_depts(stations_data, station_type)


__all__ = [
    "Slice",
    "build_slices",
    "daterange",
    "generate_fallback_slices",
    "get_active_departments_for_stations",
    "month_windows",
    "needs_truncation",
]
