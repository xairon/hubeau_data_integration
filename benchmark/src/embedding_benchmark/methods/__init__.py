"""Interface commune pour les méthodes d'embedding."""

from dataclasses import dataclass
import numpy as np
from typing import Dict


@dataclass
class MethodResult:
    """Résultat standardisé d'une méthode d'embedding."""

    station_embeddings: np.ndarray      # (n_stations, dim)
    station_ids: list[str]              # [code_bss, ...]
    window_embeddings: Dict[str, np.ndarray]  # {code_bss: (n_windows, dim)}
    elapsed_seconds: float
    method_name: str
