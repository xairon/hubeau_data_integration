"""Résultats d'une méthode d'embedding."""

from dataclasses import dataclass
import numpy as np


@dataclass
class MethodResult:
    """Résultat d'une méthode d'embedding.

    Attributes:
        station_embeddings: (n_stations, dim) array of station-level embeddings
        station_ids: list of station identifiers (code_bss for piezo, code_station for hydro)
        domains: list of domain labels ("piezo" or "hydro") per station
        window_embeddings: {station_id: (n_windows, dim)} dict of window-level embeddings
        elapsed_seconds: wall-clock time for the method
        method_name: name of the method
    """
    station_embeddings: np.ndarray
    station_ids: list[str]
    domains: list[str]
    window_embeddings: dict[str, np.ndarray]
    elapsed_seconds: float
    method_name: str
