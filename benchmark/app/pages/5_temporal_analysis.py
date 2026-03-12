"""Page 5 : Analyse temporelle des embeddings."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd
from scipy.spatial.distance import cosine
from embedding_benchmark.ui.components import (
    load_embeddings, load_window_embeddings, get_embedding_columns, has_results,
)

st.set_page_config(page_title="Analyse temporelle", page_icon="📈", layout="wide")
st.title("Analyse temporelle des embeddings")

if not has_results():
    st.error("Aucun résultat.")
    st.stop()

df_stations = load_embeddings()
df_windows = load_window_embeddings()

if df_windows.empty:
    st.warning("Pas de window embeddings. Relancez le benchmark.")
    st.stop()

emb_cols = get_embedding_columns(df_windows)

# ── Drift Leaderboard ──────────────────────────────────────────────────
st.subheader("Stations avec le plus de dérive temporelle")

# Compute drift per station
drift_scores = []
for sid, group in df_windows.groupby("station_id"):
    group = group.sort_values("window_idx")
    windows = group[emb_cols].values
    if len(windows) < 2:
        continue
    drifts = [cosine(windows[i], windows[i + 1]) for i in range(len(windows) - 1)]
    domain = group["domain"].iloc[0]
    drift_scores.append({
        "station_id": sid,
        "domain": domain,
        "max_drift": round(max(drifts), 4),
        "mean_drift": round(np.mean(drifts), 4),
        "n_windows": len(windows),
    })

drift_df = pd.DataFrame(drift_scores).sort_values("max_drift", ascending=False)

# Display top 20
st.dataframe(drift_df.head(20), use_container_width=True, hide_index=True)

fig_drift = px.bar(drift_df.head(20), x="station_id", y="max_drift", color="domain",
                   title="Top 20 stations — dérive maximale entre fenêtres consécutives")
fig_drift.update_layout(xaxis_tickangle=45)
st.plotly_chart(fig_drift, use_container_width=True)

# ── Station detail ─────────────────────────────────────────────────────
st.subheader("Évolution temporelle d'une station")

station_options = [f"[{row['domain']}] {row['station_id']}" for _, row in df_stations.iterrows()
                   if row["station_id"] in df_windows["station_id"].values]
if not station_options:
    st.stop()

selected = st.selectbox("Station", station_options)
selected_id = selected.split("] ")[1]

# Get windows for this station
station_windows = df_windows[df_windows["station_id"] == selected_id].sort_values("window_idx")
windows_emb = station_windows[emb_cols].values

if len(windows_emb) < 2:
    st.info("Pas assez de fenêtres pour cette station.")
    st.stop()

# Drift over time
drifts = [cosine(windows_emb[i], windows_emb[i + 1]) for i in range(len(windows_emb) - 1)]

fig_drift_ts = go.Figure()
fig_drift_ts.add_trace(go.Scatter(
    x=list(range(1, len(drifts) + 1)),
    y=drifts, mode="lines+markers", name="Drift (distance cosine)",
    line=dict(color="coral", width=2),
))
fig_drift_ts.update_layout(
    title=f"Dérive temporelle — {selected_id}",
    xaxis_title="Transition (fenêtre i → i+1)",
    yaxis_title="Distance cosine", height=350,
)
st.plotly_chart(fig_drift_ts, use_container_width=True)

# UMAP of this station's windows
if len(windows_emb) >= 3:
    import umap as umap_lib
    reducer = umap_lib.UMAP(n_components=2, n_neighbors=min(5, len(windows_emb) - 1),
                            random_state=42, metric="cosine")
    coords = reducer.fit_transform(windows_emb)

    fig_umap = px.scatter(
        x=coords[:, 0], y=coords[:, 1],
        color=station_windows["window_idx"].values.astype(str),
        title=f"UMAP des fenêtres — {selected_id}",
        labels=dict(x="UMAP 1", y="UMAP 2", color="Fenêtre"),
        width=700, height=500,
    )
    fig_umap.update_traces(marker=dict(size=10))
    st.plotly_chart(fig_umap, use_container_width=True)

# ── Timeline slider ────────────────────────────────────────────────────
st.subheader("UMAP global par fenêtre temporelle")

max_window = int(df_windows["window_idx"].max())
window_idx = st.slider("Index de fenêtre", 0, max_window, 0)

window_slice = df_windows[df_windows["window_idx"] == window_idx]
if len(window_slice) < 10:
    st.info(f"Seulement {len(window_slice)} stations ont une fenêtre #{window_idx}.")
else:
    X_slice = window_slice[emb_cols].values
    import umap as umap_lib
    reducer = umap_lib.UMAP(n_components=2, random_state=42, metric="cosine")
    coords = reducer.fit_transform(X_slice)

    fig_global = px.scatter(
        x=coords[:, 0], y=coords[:, 1],
        color=window_slice["domain"].values,
        hover_data=[window_slice["station_id"].values],
        title=f"UMAP de toutes les stations — fenêtre #{window_idx}",
        width=800, height=600,
    )
    fig_global.update_traces(marker=dict(size=5, opacity=0.7))
    st.plotly_chart(fig_global, use_container_width=True)
