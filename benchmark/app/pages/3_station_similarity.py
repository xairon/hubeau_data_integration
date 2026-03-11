"""Page 3 : Recherche de stations similaires (kNN dans l'espace embedding)."""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from sklearn.neighbors import NearestNeighbors
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from embedding_benchmark.ui.components import available_methods, load_embeddings, get_embedding_columns, compute_umap

st.header("🔍 Recherche de stations similaires")

methods = available_methods()
if not methods:
    st.warning("Aucun embedding disponible.")
    st.stop()

method = st.selectbox("Méthode", methods, key="sim_method")
df = load_embeddings(method)
if df.empty:
    st.stop()

# Sélection station
station = st.selectbox("Station de référence", sorted(df["code_bss"].tolist()), key="sim_station")
k = st.slider("Nombre de voisins", 3, 30, 10)

# kNN
emb_cols = get_embedding_columns(df)
embeddings = df[emb_cols].values
nn = NearestNeighbors(n_neighbors=k + 1, metric="cosine")
nn.fit(embeddings)

idx = df[df["code_bss"] == station].index[0]
distances, indices = nn.kneighbors(embeddings[idx:idx+1])

# Résultats
neighbors = df.iloc[indices[0][1:]].copy()
neighbors["distance"] = distances[0][1:]
neighbors["rank"] = range(1, k + 1)

st.subheader(f"Top {k} stations similaires à {station}")

display_cols = ["rank", "code_bss", "distance"]
for c in ["nature_eh", "milieu_eh", "cluster_id", "n_days"]:
    if c in neighbors.columns:
        display_cols.append(c)

st.dataframe(neighbors[display_cols], use_container_width=True, hide_index=True)

# Métadonnées de la station de référence
ref = df[df["code_bss"] == station].iloc[0]
st.subheader("Station de référence")
meta_cols = ["code_bss", "nature_eh", "milieu_eh", "cluster_id", "n_days"]
ref_meta = {c: ref.get(c, "N/A") for c in meta_cols if c in ref.index}
st.json(ref_meta)

# Cohérence
if "nature_eh" in neighbors.columns and "nature_eh" in ref.index:
    same_nature = (neighbors["nature_eh"] == ref["nature_eh"]).sum()
    st.metric(
        "Cohérence nature_eh",
        f"{same_nature}/{k} ({same_nature/k*100:.0f}%)",
    )

# UMAP avec highlight
coords = compute_umap(method)
if len(coords) > 0:
    df_plot = df.copy()
    df_plot["umap_1"] = coords[:, 0]
    df_plot["umap_2"] = coords[:, 1]
    df_plot["role"] = "autre"
    df_plot.loc[df_plot["code_bss"] == station, "role"] = "référence"
    df_plot.loc[df_plot["code_bss"].isin(neighbors["code_bss"]), "role"] = "voisin"

    fig = px.scatter(
        df_plot, x="umap_1", y="umap_2",
        color="role",
        color_discrete_map={"autre": "lightgray", "référence": "red", "voisin": "blue"},
        hover_data=["code_bss", "nature_eh"],
        title=f"UMAP — {station} et ses {k} voisins ({method})",
        opacity=0.7,
        height=600,
    )
    fig.update_traces(marker=dict(size=5), selector=dict(name="autre"))
    fig.update_traces(marker=dict(size=12), selector=dict(name="référence"))
    fig.update_traces(marker=dict(size=8), selector=dict(name="voisin"))
    st.plotly_chart(fig, use_container_width=True)
