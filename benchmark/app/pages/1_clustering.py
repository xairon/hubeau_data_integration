"""Page 1 : Clustering & Exploration UMAP."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import streamlit as st
import plotly.express as px
import numpy as np
from embedding_benchmark.ui.components import (
    load_embeddings, compute_umap, get_embedding_matrix, has_results,
)
from embedding_benchmark.evaluation import cluster_hdbscan, eval_silhouette

st.set_page_config(page_title="Clustering", page_icon="🔬", layout="wide")
st.title("Clustering & Exploration")

if not has_results():
    st.error("Aucun résultat. Lancez `python scripts/run_softclt.py` d'abord.")
    st.stop()

df = load_embeddings()
if df.empty:
    st.stop()

# Sidebar controls
st.sidebar.header("Paramètres")
min_cluster_size = st.sidebar.slider("HDBSCAN min_cluster_size", 3, 50, 10)
min_samples = st.sidebar.slider("HDBSCAN min_samples", 1, 30, 5)
color_by = st.sidebar.selectbox("Colorer par", ["cluster", "domain", "nature_eh", "type_site", "code_departement", "code_region"])
domain_filter = st.sidebar.multiselect("Filtrer par domaine", ["piezo", "hydro"], default=["piezo", "hydro"])

# Filter
mask = df["domain"].isin(domain_filter)
df_filtered = df[mask].reset_index(drop=True)

if len(df_filtered) < 5:
    st.warning("Trop peu de stations après filtrage.")
    st.stop()

# Re-cluster on filtered data
X = get_embedding_matrix(df_filtered)
labels = cluster_hdbscan(X, min_cluster_size=min_cluster_size, min_samples=min_samples)
df_filtered["cluster"] = labels.astype(str)
sil = eval_silhouette(X, labels)

# UMAP
umap_coords = compute_umap()
umap_filtered = umap_coords[mask] if len(umap_coords) == len(df) else None

if umap_filtered is None or len(umap_filtered) != len(df_filtered):
    import umap as umap_lib
    reducer = umap_lib.UMAP(n_components=2, random_state=42, metric="cosine")
    umap_filtered = reducer.fit_transform(X)

df_filtered["umap_x"] = umap_filtered[:, 0]
df_filtered["umap_y"] = umap_filtered[:, 1]

# Color column
if color_by == "cluster":
    color_col = "cluster"
elif color_by in df_filtered.columns:
    color_col = color_by
    df_filtered[color_col] = df_filtered[color_col].fillna("inconnu").astype(str)
else:
    st.warning(f"Colonne '{color_by}' non disponible pour ce domaine.")
    color_col = "domain"

# Stats
n_clusters = len(set(labels[labels >= 0]))
n_noise = int((labels == -1).sum())

col1, col2, col3, col4 = st.columns(4)
col1.metric("Stations", len(df_filtered))
col2.metric("Clusters", n_clusters)
col3.metric("Noise", f"{n_noise} ({100*n_noise/len(df_filtered):.0f}%)")
col4.metric("Silhouette", f"{sil:.3f}" if sil > -1 else "N/A")

# UMAP plot
fig = px.scatter(
    df_filtered, x="umap_x", y="umap_y", color=color_col,
    hover_data=["station_id", "domain", "cluster"],
    title=f"UMAP — coloré par {color_by}",
    width=900, height=600,
)
fig.update_traces(marker=dict(size=5, opacity=0.7))
fig.update_layout(legend=dict(orientation="h", yanchor="bottom", y=-0.2))
st.plotly_chart(fig, use_container_width=True)

# Cluster distribution
if n_clusters > 0:
    st.subheader("Distribution des clusters")
    dist = df_filtered[df_filtered["cluster"] != "-1"].groupby(["cluster", "domain"]).size().reset_index(name="count")
    fig2 = px.bar(dist, x="cluster", y="count", color="domain", barmode="group",
                  title="Nombre de stations par cluster et domaine")
    st.plotly_chart(fig2, use_container_width=True)
