"""Configuration centralisée pour le benchmark."""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class BenchmarkConfig:
    """Configuration du benchmark, lue depuis .env ou valeurs par défaut."""

    # DB (lecture seule)
    pg_host: str = field(default_factory=lambda: os.getenv("PG_HOST", "localhost"))
    pg_port: int = field(default_factory=lambda: int(os.getenv("PG_PORT", "49502")))
    pg_db: str = field(default_factory=lambda: os.getenv("PG_DB", "postgres"))
    pg_user: str = field(default_factory=lambda: os.getenv("PG_USER", "postgres"))
    pg_password: str = field(default_factory=lambda: os.getenv("PG_PASSWORD", ""))

    # Benchmark params
    sample_size: int = field(default_factory=lambda: int(os.getenv("SAMPLE_SIZE", "300")))
    piezo_sample_size: int = field(default_factory=lambda: int(os.getenv("PIEZO_SAMPLE_SIZE", "1000")))
    hydro_sample_size: int = field(default_factory=lambda: int(os.getenv("HYDRO_SAMPLE_SIZE", "1000")))
    window_size: int = field(default_factory=lambda: int(os.getenv("WINDOW_SIZE", "365")))
    stride: int = field(default_factory=lambda: int(os.getenv("STRIDE", "90")))
    embedding_dim: int = field(default_factory=lambda: int(os.getenv("EMBEDDING_DIM", "320")))
    seed: int = field(default_factory=lambda: int(os.getenv("SEED", "42")))

    # Variables piézo (measurement + ERA5)
    piezo_cols: list[str] = field(default_factory=lambda: [
        "niveau_nappe_eau", "temperature_2m", "total_precipitation", "potential_evaporation"
    ])

    # Variables hydro (measurement + ERA5)
    hydro_cols: list[str] = field(default_factory=lambda: [
        "resultat_obs_elab", "temperature_2m", "total_precipitation", "potential_evaporation"
    ])

    # ERA5 columns (shared)
    era5_cols: list[str] = field(default_factory=lambda: [
        "temperature_2m", "total_precipitation", "potential_evaporation"
    ])

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_db}"

    @property
    def results_dir(self):
        from pathlib import Path
        d = Path(__file__).parent.parent.parent / "results"
        d.mkdir(parents=True, exist_ok=True)
        return d

    @property
    def embeddings_dir(self):
        d = self.results_dir / "embeddings"
        d.mkdir(parents=True, exist_ok=True)
        return d

    @property
    def windows_dir(self):
        d = self.results_dir / "windows"
        d.mkdir(parents=True, exist_ok=True)
        return d

    @property
    def metrics_dir(self):
        d = self.results_dir / "metrics"
        d.mkdir(parents=True, exist_ok=True)
        return d

    @property
    def models_dir(self):
        d = self.results_dir / "models"
        d.mkdir(parents=True, exist_ok=True)
        return d


# Singleton
cfg = BenchmarkConfig()
