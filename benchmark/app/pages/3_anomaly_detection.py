"""Page 3 : Détection d'anomalies."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd
from embedding_benchmark.ui.components import (
    load_embeddings, compute_umap, get_embedding_matrix, has_results,
)
from embedding_benchmark.anomaly import (
    detect_anomalies_iforest, detect_anomalies_lof, build_anomaly_table,
)
from embedding_benchmark.data_loader import load_piezo_series, load_hydro_series

st.set_page_config(page_title="Anomalies", page_icon="⚠️", layout="wide")
st.title("Détection d'anomalies")

if not has_results():
    st.error("Aucun résultat.")
    st.stop()

df = load_embeddings()
if df.empty:
    st.stop()

X = get_embedding_matrix(df)
station_ids = df["station_id"].tolist()
domains = df["domain"].tolist()

# Controls
st.sidebar.header("Paramètres")
method = st.sidebar.selectbox("Méthode", ["Isolation Forest", "LOF"])
contamination = st.sidebar.slider("Contamination (%)", 1, 20, 5) / 100

# Detect
if method == "Isolation Forest":
    scores = detect_anomalies_iforest(X, contamination=contamination)
else:
    scores = detect_anomalies_lof(X, contamination=contamination)

threshold = np.percentile(scores, contamination * 100)
anomaly_mask = scores <= threshold

# Stats
n_anomalies = anomaly_mask.sum()
col1, col2, col3 = st.columns(3)
col1.metric("Stations totales", len(df))
col2.metric("Anomalies détectées", n_anomalies)
col3.metric("Taux", f"{100*n_anomalies/len(df):.1f}%")

# UMAP with anomalies
umap_coords = compute_umap()
if len(umap_coords) == len(df):
    df_plot = df.copy()
    df_plot["umap_x"] = umap_coords[:, 0]
    df_plot["umap_y"] = umap_coords[:, 1]
    df_plot["anomaly"] = np.where(anomaly_mask, "anomalie", "normal")
    df_plot["score"] = scores

    fig = px.scatter(df_plot, x="umap_x", y="umap_y", color="anomaly",
                     color_discrete_map={"normal": "lightblue", "anomalie": "red"},
                     hover_data=["station_id", "domain", "score"],
                     title=f"UMAP — {method} (contamination={contamination:.0%})",
                     width=900, height=600)
    fig.update_traces(marker=dict(size=4, opacity=0.5), selector=dict(name="normal"))
    fig.update_traces(marker=dict(size=10, opacity=1.0), selector=dict(name="anomalie"))
    st.plotly_chart(fig, use_container_width=True)

# Anomaly table
st.subheader("Stations anomaliques")
anomaly_table = build_anomaly_table(X, station_ids, domains, scores, contamination)
st.dataframe(anomaly_table, use_container_width=True, hide_index=True)

# Compare anomaly vs normal
if len(anomaly_table) > 0:
    st.subheader("Comparaison : anomalie vs station normale la plus proche")
    selected_anom = st.selectbox("Station anomalique",
                                  anomaly_table["station_id"].tolist())
    row = anomaly_table[anomaly_table["station_id"] == selected_anom].iloc[0]
    normal_id = row["nearest_normal_id"]

    if normal_id:
        fig_ts = go.Figure()
        for sid, label, color in [(selected_anom, f"Anomalie: {selected_anom}", "red"),
                                   (normal_id, f"Normal: {normal_id}", "blue")]:
            dom = row["domain"] if sid == selected_anom else row["nearest_normal_domain"]
            try:
                if dom == "piezo":
                    s, _ = load_piezo_series([sid])
                else:
                    s, _ = load_hydro_series([sid])
                if sid in s:
                    arr = s[sid][:, 0]
                    arr = (arr - arr.mean()) / (arr.std() + 1e-8)
                    fig_ts.add_trace(go.Scatter(y=arr[-365*2:], name=label, mode="lines",
                                                line=dict(color=color, width=2)))
            except Exception:
                pass

        fig_ts.update_layout(title="Séries normalisées (2 dernières années)", height=400)
        st.plotly_chart(fig_ts, use_container_width=True)
