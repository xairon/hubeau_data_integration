"""Composants UI réutilisables pour le benchmark Streamlit."""

import json
import numpy as np
import pandas as pd
import streamlit as st
from pathlib import Path

from embedding_benchmark.config import cfg


def load_embeddings(name: str = "SoftCLT_unified") -> pd.DataFrame:
    """Load station embeddings from parquet."""
    path = cfg.embeddings_dir / f"{name}.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def load_window_embeddings(name: str = "SoftCLT_unified") -> pd.DataFrame:
    """Load window-level embeddings from parquet."""
    path = cfg.windows_dir / f"{name}_windows.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def load_metrics(name: str = "SoftCLT_unified") -> dict:
    """Load metrics JSON."""
    path = cfg.metrics_dir / f"{name}.json"
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)


def get_embedding_columns(df: pd.DataFrame) -> list[str]:
    """Return embedding dimension column names."""
    return [c for c in df.columns if c.startswith("emb_")]


def get_embedding_matrix(df: pd.DataFrame) -> np.ndarray:
    """Extract embedding matrix from DataFrame."""
    return df[get_embedding_columns(df)].values


@st.cache_data(ttl=3600)
def compute_umap(name: str = "SoftCLT_unified", n_components: int = 2) -> np.ndarray:
    """Compute cached UMAP projection."""
    import umap
    df = load_embeddings(name)
    if df.empty:
        return np.array([])
    X = get_embedding_matrix(df)
    reducer = umap.UMAP(n_components=n_components, random_state=42, metric="cosine")
    return reducer.fit_transform(X)


def has_results(name: str = "SoftCLT_unified") -> bool:
    """Check if results exist for the given run."""
    return (cfg.embeddings_dir / f"{name}.parquet").exists()
