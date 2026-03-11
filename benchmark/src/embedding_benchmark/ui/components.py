"""Composants partagés pour le chargement des résultats et la projection UMAP."""

import json
import numpy as np
import pandas as pd
import streamlit as st
from pathlib import Path
from umap import UMAP

from ..config import cfg


METRICS_DIR = cfg.metrics_dir
EMBEDDINGS_DIR = cfg.embeddings_dir


def load_all_metrics() -> pd.DataFrame:
    """Charge tous les JSON de métriques en un DataFrame."""
    records = []
    for f in sorted(METRICS_DIR.glob("*.json")):
        if f.name == "summary.json":
            continue
        with open(f) as fp:
            records.append(json.load(fp))
    return pd.DataFrame(records)


def load_embeddings(method_name: str) -> pd.DataFrame:
    """Charge le parquet d'embeddings d'une méthode."""
    path = EMBEDDINGS_DIR / f"{method_name}.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def get_embedding_columns(df: pd.DataFrame) -> list[str]:
    """Retourne la liste des colonnes embedding (emb_0, emb_1, ...)."""
    return [c for c in df.columns if c.startswith("emb_")]


@st.cache_data(ttl=3600)
def compute_umap(method_name: str, n_components: int = 2) -> np.ndarray:
    """Calcule et cache la projection UMAP pour une méthode (cache Streamlit, TTL 1h)."""
    df = load_embeddings(method_name)
    if df.empty:
        return np.array([])
    emb_cols = get_embedding_columns(df)
    embeddings = df[emb_cols].values
    reducer = UMAP(n_components=n_components, random_state=42, metric="cosine")
    return reducer.fit_transform(embeddings)


def available_methods() -> list[str]:
    """Liste les méthodes pour lesquelles on a des embeddings."""
    return sorted([f.stem for f in EMBEDDINGS_DIR.glob("*.parquet")])
