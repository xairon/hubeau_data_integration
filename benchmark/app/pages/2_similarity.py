"""Page 2 : Recherche de similarité (kNN)."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd
from sklearn.neighbors import NearestNeighbors
from embedding_benchmark.ui.components import (
    load_embeddings, compute_umap, get_embedding_matrix, has_results,
)
from embedding_benchmark.data_loader import load_piezo_series, load_hydro_series
from embedding_benchmark.config import cfg

st.set_page_config(page_title="Similarité", page_icon="🔍", layout="wide")
st.title("Recherche de similarité")

if not has_results():
    st.error("Aucun résultat.")
    st.stop()

df = load_embeddings()
if df.empty:
    st.stop()

X = get_embedding_matrix(df)

# Station selector
station_options = [f"[{row['domain']}] {row['station_id']}" for _, row in df.iterrows()]
selected = st.selectbox("Station de référence", station_options)
selected_idx = station_options.index(selected)
selected_id = df.iloc[selected_idx]["station_id"]
selected_domain = df.iloc[selected_idx]["domain"]

# K slider
max_k = min(50, len(df) - 1)
k = st.slider("Nombre de voisins (K)", 1, max(1, max_k), min(10, max_k))

# kNN search
nn = NearestNeighbors(n_neighbors=min(k + 1, len(df)), metric="cosine")
nn.fit(X)
distances, indices = nn.kneighbors(X[selected_idx:selected_idx + 1])

neighbors = []
for rank, (idx, dist) in enumerate(zip(indices[0][1:], distances[0][1:])):
    row = df.iloc[idx]
    neighbors.append({
        "Rang": rank + 1,
        "Station": row["station_id"],
        "Domaine": row["domain"],
        "Distance cosine": round(dist, 4),
        **{c: row.get(c, "") for c in ["nature_eh", "type_site", "code_departement"] if c in df.columns},
    })

# Display
col1, col2 = st.columns([1, 2])

with col1:
    st.subheader(f"Référence: {selected_id}")
    ref_meta = df.iloc[selected_idx]
    for c in ["domain", "nature_eh", "type_site", "code_departement", "code_region"]:
        if c in ref_meta and pd.notna(ref_meta[c]):
            st.write(f"**{c}**: {ref_meta[c]}")

    # Cross-domain count
    neighbor_df = pd.DataFrame(neighbors)
    cross = neighbor_df[neighbor_df["Domaine"] != selected_domain]
    if len(cross) > 0:
        st.success(f"🔀 {len(cross)}/{k} voisins cross-domaine !")

    st.dataframe(neighbor_df, use_container_width=True, hide_index=True)

with col2:
    # UMAP with highlight
    umap_coords = compute_umap()
    if len(umap_coords) == len(df):
        df_plot = df.copy()
        df_plot["umap_x"] = umap_coords[:, 0]
        df_plot["umap_y"] = umap_coords[:, 1]

        # Mark reference + neighbors
        df_plot["role"] = "other"
        df_plot.loc[selected_idx, "role"] = "reference"
        for idx in indices[0][1:]:
            df_plot.loc[idx, "role"] = "neighbor"

        fig = px.scatter(df_plot, x="umap_x", y="umap_y", color="role",
                         color_discrete_map={"other": "lightgrey", "reference": "red", "neighbor": "blue"},
                         hover_data=["station_id", "domain"],
                         title="UMAP — référence (rouge) et voisins (bleu)",
                         width=800, height=600)
        fig.update_traces(marker=dict(size=4, opacity=0.5), selector=dict(name="other"))
        fig.update_traces(marker=dict(size=10, opacity=1.0), selector=dict(name="reference"))
        fig.update_traces(marker=dict(size=8, opacity=0.9), selector=dict(name="neighbor"))
        st.plotly_chart(fig, use_container_width=True)

# Time series comparison
st.subheader("Comparaison des séries temporelles")
n_show = st.slider("Nombre de voisins à afficher", 1, min(k, 5), min(3, k), key="ts_show")

stations_to_load = [selected_id] + [neighbors[i]["Station"] for i in range(n_show)]
domains_to_load = [selected_domain] + [neighbors[i]["Domaine"] for i in range(n_show)]

# Load and plot time series (normalized for comparison)
fig_ts = go.Figure()
for sid, dom in zip(stations_to_load, domains_to_load):
    try:
        if dom == "piezo":
            s, _ = load_piezo_series([sid])
            col_name = "niveau_nappe_eau"
        else:
            s, _ = load_hydro_series([sid])
            col_name = "resultat_obs_elab"
        if sid in s:
            arr = s[sid][:, 0]  # first column = measurement
            # Z-score normalize for comparison
            arr = (arr - arr.mean()) / (arr.std() + 1e-8)
            label = f"{'⭐ ' if sid == selected_id else ''}{sid} [{dom}]"
            fig_ts.add_trace(go.Scatter(y=arr[-365*2:], name=label, mode="lines",
                                        line=dict(width=3 if sid == selected_id else 1)))
    except Exception:
        pass

fig_ts.update_layout(title="Séries normalisées (2 dernières années)", height=400,
                     xaxis_title="Jours", yaxis_title="Z-score")
st.plotly_chart(fig_ts, use_container_width=True)
