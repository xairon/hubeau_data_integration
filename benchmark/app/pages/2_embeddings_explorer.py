"""Page 2 : Exploration interactive UMAP des embeddings."""

import streamlit as st
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from embedding_benchmark.ui.components import available_methods, load_embeddings, compute_umap

st.header("🗺️ Exploration des embeddings")

methods = available_methods()
if not methods:
    st.warning("Aucun embedding disponible.")
    st.stop()

# Sélection
col1, col2 = st.columns(2)
with col1:
    method = st.selectbox("Méthode", methods)
with col2:
    color_by = st.selectbox("Colorer par", ["cluster_id", "nature_eh", "milieu_eh", "n_days"])

# Charger
df = load_embeddings(method)
if df.empty:
    st.warning(f"Pas d'embeddings pour {method}")
    st.stop()

# UMAP
coords = compute_umap(method)
if len(coords) == 0:
    st.error("UMAP échoué")
    st.stop()

df["umap_1"] = coords[:, 0]
df["umap_2"] = coords[:, 1]

# Préparer la colonne couleur
if color_by not in df.columns:
    st.warning(f"Colonne '{color_by}' absente")
    color_by = "cluster_id"

if color_by in ["cluster_id"]:
    df[color_by] = df[color_by].astype(str)

# Plot
fig = px.scatter(
    df, x="umap_1", y="umap_2",
    color=color_by,
    hover_data=["code_bss", "nature_eh", "milieu_eh", "n_days"],
    title=f"{method} — UMAP (coloré par {color_by})",
    width=900, height=700,
    opacity=0.7,
)
fig.update_traces(marker=dict(size=5))
fig.update_layout(legend=dict(itemsizing="constant"))

st.plotly_chart(fig, use_container_width=True)

# Stats
st.subheader("Statistiques")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Stations", len(df))
with col2:
    n_clusters = df["cluster_id"].nunique() if "cluster_id" in df.columns else 0
    st.metric("Clusters", n_clusters)
with col3:
    noise = (df.get("cluster_id", "").astype(str) == "-1").sum()
    st.metric("Noise", f"{noise} ({noise/len(df)*100:.1f}%)")

# Distribution clusters
if "cluster_id" in df.columns:
    st.subheader("Distribution des clusters")
    cluster_counts = df["cluster_id"].value_counts().reset_index()
    cluster_counts.columns = ["cluster_id", "count"]
    fig_bar = px.bar(cluster_counts, x="cluster_id", y="count", title="Stations par cluster")
    st.plotly_chart(fig_bar, use_container_width=True)
