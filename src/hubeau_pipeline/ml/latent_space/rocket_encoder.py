"""MiniRocket-based encoder for univariate time series embeddings.

Uses random convolutional kernels (MiniRocket) + PCA to produce
fixed-dimension embeddings from sliding windows of univariate series.
No training required — deterministic with random_state.
"""

import logging
import numpy as np
import joblib
from pathlib import Path

logger = logging.getLogger(__name__)


class RocketEncoder:
    """MiniRocket + PCA encoder for univariate time series.

    Pipeline: sliding windows → MiniRocket (9,996 features) → PCA (320d) → mean pool.
    No GPU required, deterministic with random_state.
    """

    def __init__(
        self,
        embedding_dim: int = 320,
        window_size: int = 365,
        stride: int = 90,
        random_state: int = 42,
    ):
        self.embedding_dim = embedding_dim
        self.window_size = window_size
        self.stride = stride
        self.random_state = random_state
        self.rocket = None
        self.pca = None
        self.scaler = None

    def fit(self, series_dict: dict[str, np.ndarray], dagster_context=None):
        """Fit MiniRocket + PCA on a collection of univariate series.

        Args:
            series_dict: {station_id: (T, 1) array}
            dagster_context: Optional Dagster context for logging.
        """
        from aeon.transformations.collection.convolution_based import MiniRocket
        from sklearn.decomposition import PCA
        from sklearn.preprocessing import StandardScaler

        def _log(msg):
            if dagster_context:
                dagster_context.log.info(msg)
            else:
                logger.info(msg)

        # Build sliding windows
        windows = []
        for arr in series_dict.values():
            T = len(arr)
            for start in range(0, T - self.window_size + 1, self.stride):
                windows.append(arr[start:start + self.window_size, 0])

        X = np.array(windows)[:, np.newaxis, :]  # (N, 1, window_size)
        _log(f"RocketEncoder: {len(series_dict)} stations → {X.shape[0]} windows")

        # Fit MiniRocket on a subset (random kernels, doesn't need all data)
        fit_size = min(5000, X.shape[0])
        self.rocket = MiniRocket(random_state=self.random_state)
        self.rocket.fit(X[:fit_size])
        _log(f"MiniRocket fitted on {fit_size} windows")

        # Transform in batches to limit memory (~9996 floats × batch_size)
        BATCH = 20000
        n_total = X.shape[0]

        # First pass: fit scaler + PCA on a subset
        subset_size = min(30000, n_total)
        subset_features = self.rocket.transform(X[:subset_size]).astype(np.float32)
        _log(f"MiniRocket subset transform: {subset_size} windows → {subset_features.shape}")

        self.scaler = StandardScaler()
        subset_scaled = self.scaler.fit_transform(subset_features)

        self.pca = PCA(n_components=self.embedding_dim, random_state=self.random_state)
        self.pca.fit(subset_scaled)
        explained = self.pca.explained_variance_ratio_.cumsum()
        _log(
            f"PCA fitted on {subset_size} windows: {subset_features.shape[1]}d → {self.embedding_dim}d, "
            f"explained variance: {explained[-1]:.2%}"
        )
        del subset_features, subset_scaled  # free memory

        return self

    def encode_windows(
        self,
        series: np.ndarray,
        dates: list | None = None,
    ) -> tuple[np.ndarray, list[tuple[str, str]]]:
        """Encode a single univariate series into window embeddings.

        Args:
            series: (T, 1) scaled array.
            dates: Optional date list for window date ranges.

        Returns:
            (n_windows, embedding_dim) array and [(start_date, end_date), ...]
        """
        T = len(series)
        windows = []
        window_dates = []

        for start in range(0, T - self.window_size + 1, self.stride):
            end = start + self.window_size
            windows.append(series[start:end, 0])
            if dates is not None:
                window_dates.append((str(dates[start]), str(dates[end - 1])))

        if not windows:
            return np.zeros((0, self.embedding_dim)), []

        X = np.array(windows)[:, np.newaxis, :]  # (n_windows, 1, window_size)
        features = self.rocket.transform(X)
        features_scaled = self.scaler.transform(features)
        embeddings = self.pca.transform(features_scaled)

        return embeddings, window_dates

    @staticmethod
    def station_embedding(window_embeddings: np.ndarray) -> np.ndarray:
        """Mean pooling of window embeddings to get station embedding."""
        return window_embeddings.mean(axis=0)

    def save(self, path: Path) -> None:
        """Save encoder components to directory."""
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        joblib.dump(self.rocket, path / "rocket.pkl")
        joblib.dump(self.pca, path / "pca.pkl")
        joblib.dump(self.scaler, path / "scaler.pkl")
        joblib.dump(
            {"embedding_dim": self.embedding_dim, "window_size": self.window_size,
             "stride": self.stride, "random_state": self.random_state},
            path / "config.pkl",
        )
        logger.info(f"RocketEncoder saved to {path}")

    @classmethod
    def load(cls, path: Path) -> "RocketEncoder":
        """Load encoder from directory."""
        path = Path(path)
        config = joblib.load(path / "config.pkl")
        enc = cls(**config)
        enc.rocket = joblib.load(path / "rocket.pkl")
        enc.pca = joblib.load(path / "pca.pkl")
        enc.scaler = joblib.load(path / "scaler.pkl")
        logger.info(f"RocketEncoder loaded from {path}")
        return enc
