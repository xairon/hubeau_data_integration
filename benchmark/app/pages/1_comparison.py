"""Page 1 : Tableau comparatif + radar chart."""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from embedding_benchmark.ui.components import load_all_metrics

st.header("📊 Comparaison des méthodes")

df = load_all_metrics()
if df.empty:
    st.warning("Aucun résultat. Exécuter le benchmark d'abord.")
    st.stop()

# --- Tableau ---
st.subheader("Métriques")

metric_cols = ["method", "n_clusters", "noise_pct", "silhouette",
               "ari_nature_eh", "temporal_stability", "knn_coherence_nature_eh"]
if "time_seconds" in df.columns:
    metric_cols.append("time_seconds")
available_cols = [c for c in metric_cols if c in df.columns]

st.dataframe(
    df[available_cols].sort_values("silhouette", ascending=False),
    use_container_width=True,
    hide_index=True,
)

# --- Score composite ---
st.subheader("Score composite")

weights = {
    "silhouette": st.sidebar.slider("Poids Silhouette", 0.0, 1.0, 0.3, 0.05),
    "ari_nature_eh": st.sidebar.slider("Poids ARI nature_eh", 0.0, 1.0, 0.3, 0.05),
    "temporal_stability": st.sidebar.slider("Poids Stabilité temp.", 0.0, 1.0, 0.2, 0.05),
    "knn_coherence_nature_eh": st.sidebar.slider("Poids kNN cohérence", 0.0, 1.0, 0.2, 0.05),
}

df_score = df.copy()
for col in weights:
    if col in df_score.columns:
        mi, ma = df_score[col].min(), df_score[col].max()
        df_score[col + "_norm"] = (df_score[col] - mi) / (ma - mi) if ma > mi else 0.5

total_weight = sum(weights.values())
df_score["score_composite"] = sum(
    df_score.get(col + "_norm", 0.5) * (w / total_weight)
    for col, w in weights.items()
)
df_score = df_score.sort_values("score_composite", ascending=False)

st.dataframe(
    df_score[["method", "score_composite"]].reset_index(drop=True),
    use_container_width=True,
    hide_index=True,
)

st.success(f"🏆 Méthode recommandée : **{df_score.iloc[0]['method']}** (score = {df_score.iloc[0]['score_composite']:.4f})")

# --- Radar chart ---
st.subheader("Radar chart")

metrics = list(weights.keys())
labels = ["Silhouette", "ARI nature_eh", "Stabilité temp.", "kNN cohérence"]

fig = go.Figure()
for _, row in df_score.iterrows():
    values = [row.get(m + "_norm", 0.5) for m in metrics]
    values.append(values[0])  # fermer le polygone
    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=labels + [labels[0]],
        fill="toself",
        name=row["method"],
        opacity=0.6,
    ))

fig.update_layout(
    polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
    showlegend=True,
    height=500,
)
st.plotly_chart(fig, use_container_width=True)
