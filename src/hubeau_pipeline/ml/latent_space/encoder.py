"""SoftCLT encoder wrapper for hydrological time series."""

import logging
import time
import numpy as np
import torch
import joblib
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def _patch_softclt_loss():
    """Monkey-patch TS2Vec loss with SoftCLT hierarchical contrastive loss."""
    from ..softclt.losses import hierarchical_contrastive_loss
    from .. import ts2vec
    ts2vec.losses.hierarchical_contrastive_loss = hierarchical_contrastive_loss
    from ..ts2vec import ts2vec as ts2vec_module
    ts2vec_module.hierarchical_contrastive_loss = hierarchical_contrastive_loss


def _resolve_device(device: str) -> str:
    if device == "auto":
        return "cuda" if torch.cuda.is_available() else "cpu"
    return device


class _EarlyStopSignal(Exception):
    """Internal signal to break out of TS2Vec's training loop."""


class SoftCLTEncoder:
    """SoftCLT encoder for multivariate hydrological time series.

    Wraps TS2Vec with SoftCLT loss monkey-patch and handles
    training, encoding, and model persistence.
    """

    def __init__(
        self,
        input_dims: int = 4,
        embedding_dim: int = 320,
        hidden_dim: int = 320,
        depth: int = 10,
        device: str = "auto",
    ):
        self.device = _resolve_device(device)
        self.model = None
        self.input_dims = input_dims
        self.embedding_dim = embedding_dim
        self.hidden_dim = hidden_dim
        self.depth = depth

    def fit(
        self,
        train_series: list[np.ndarray],
        n_epochs: int = 200,
        lr: float = 1e-3,
        batch_size: int = 128,
        max_train_length: int = 1500,
        early_stop_patience: int = 20,
        dagster_context=None,
    ) -> "SoftCLTEncoder":
        """Train SoftCLT on a list of multivariate series.

        Args:
            early_stop_patience: Stop training if loss doesn't improve for N epochs.
                Set to 0 to disable early stopping.
            dagster_context: Optional AssetExecutionContext for Dagster event logging.
        """
        from ..ts2vec.ts2vec import TS2Vec

        _patch_softclt_loss()

        # Training progress tracker with early stopping
        training_state = {
            "start_time": time.time(),
            "n_epochs": n_epochs,
            "best_loss": float("inf"),
            "patience_counter": 0,
            "best_epoch": 0,
        }

        def _log(msg):
            if dagster_context is not None:
                dagster_context.log.info(msg)
            else:
                logger.info(msg)

        def _epoch_callback(model, loss):
            epoch = model.n_epochs
            elapsed = time.time() - training_state["start_time"]
            avg_sec = elapsed / max(epoch, 1)
            remaining = avg_sec * (training_state["n_epochs"] - epoch)

            if loss < training_state["best_loss"]:
                training_state["best_loss"] = loss
                training_state["patience_counter"] = 0
                training_state["best_epoch"] = epoch
            else:
                training_state["patience_counter"] += 1

            # Log every 10 epochs or at milestones
            if epoch % 10 == 0 or epoch == 1 or epoch == training_state["n_epochs"]:
                _log(
                    f"Epoch {epoch}/{training_state['n_epochs']} — "
                    f"loss={loss:.6f} (best={training_state['best_loss']:.6f} @{training_state['best_epoch']}) — "
                    f"{elapsed:.0f}s elapsed, ~{remaining:.0f}s remaining"
                )

            # Early stopping
            if early_stop_patience > 0 and training_state["patience_counter"] >= early_stop_patience:
                _log(
                    f"Early stopping at epoch {epoch} — no improvement for {early_stop_patience} epochs "
                    f"(best={training_state['best_loss']:.6f} @epoch {training_state['best_epoch']})"
                )
                raise _EarlyStopSignal()

        self.model = TS2Vec(
            input_dims=self.input_dims,
            output_dims=self.embedding_dim,
            hidden_dims=self.hidden_dim,
            depth=self.depth,
            device=self.device,
            lr=lr,
            batch_size=batch_size,
            max_train_length=max_train_length,
            after_epoch_callback=_epoch_callback,
        )
        try:
            self.model.fit(train_series, n_epochs=n_epochs, verbose=False)
        except _EarlyStopSignal:
            pass  # Training stopped early, model is usable

        total = time.time() - training_state["start_time"]
        _log(f"Training done: {training_state['best_epoch']} best epoch, {total:.0f}s total")
        return self


    def encode_windows(
        self,
        series: np.ndarray,
        window_size: int = 365,
        stride: int = 90,
        dates: Optional[list] = None,
    ) -> tuple[np.ndarray, list[tuple[str, str]]]:
        """Encode a series into sliding window embeddings.

        Returns:
            (n_windows, embedding_dim) array and [(start_date, end_date), ...]
        """
        T = len(series)
        embeddings = []
        window_dates = []

        for start in range(0, T - window_size + 1, stride):
            end = start + window_size
            window = series[start:end]
            emb = self.model.encode(window[np.newaxis].astype(np.float32), encoding_window="full_series")
            embeddings.append(emb.squeeze())
            if dates is not None:
                window_dates.append((str(dates[start]), str(dates[end - 1])))

        return np.stack(embeddings), window_dates

    @staticmethod
    def station_embedding(window_embeddings: np.ndarray) -> np.ndarray:
        """Mean pooling of window embeddings to get station embedding."""
        return window_embeddings.mean(axis=0)

    def save(self, path: Path) -> None:
        """Save model weights to file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.model.save(str(path))

    @classmethod
    def load(cls, model_path: Path, device: str = "auto") -> "SoftCLTEncoder":
        """Load a trained model from file."""
        from ..ts2vec.ts2vec import TS2Vec

        _patch_softclt_loss()

        enc = cls.__new__(cls)
        enc.device = _resolve_device(device)
        # Need to know dims to reconstruct — infer from state dict
        state_dict = torch.load(str(model_path), map_location=enc.device)
        # Infer dims from state dict keys
        input_fc_key = [k for k in state_dict if "input_fc.weight" in k][0]
        hidden_dim, input_dim = state_dict[input_fc_key].shape
        # Count conv blocks and find max index
        block_indices = set()
        for k in state_dict:
            if "feature_extractor.net." in k:
                idx = k.split("feature_extractor.net.")[1].split(".")[0]
                block_indices.add(int(idx))
        # DilatedConvEncoder creates [hidden]*depth + [output] = depth+1 blocks
        depth = len(block_indices) - 1
        # Output dim from final block's conv2
        max_block = max(block_indices)
        output_dim = state_dict[f"module.feature_extractor.net.{max_block}.conv2.conv.weight"].shape[0]

        enc.input_dims = input_dim
        enc.embedding_dim = output_dim
        enc.hidden_dim = hidden_dim
        enc.depth = depth

        enc.model = TS2Vec(
            input_dims=input_dim,
            output_dims=output_dim,
            hidden_dims=hidden_dim,
            depth=depth,
            device=enc.device,
        )
        enc.model.load(str(model_path))
        return enc
