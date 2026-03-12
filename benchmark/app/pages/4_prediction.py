"""Page 4 : Prédiction downstream."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd
from embedding_benchmark.ui.components import (
    load_embeddings, get_embedding_matrix, has_results,
)
from embedding_benchmark.prediction import run_all_predictions

st.set_page_config(page_title="Prédiction", page_icon="🎯", layout="wide")
st.title("Prédiction downstream")
st.markdown("Les embeddings capturent-ils de l'information sémantique ? Testons-les comme features de classifieurs.")

if not has_results():
    st.error("Aucun résultat.")
    st.stop()

df = load_embeddings()
if df.empty:
    st.stop()

X = get_embedding_matrix(df)
station_ids = df["station_id"].tolist()
domains = df["domain"].tolist()

# Run predictions (cached)
@st.cache_data
def cached_predictions():
    return run_all_predictions(X, station_ids, domains, df)

with st.spinner("Entraînement des classifieurs..."):
    results = cached_predictions()

if not results:
    st.warning("Aucune tâche de prédiction possible avec les données disponibles.")
    st.stop()

# Display results per task
for task_result in results:
    task_name = task_result["task"]
    st.subheader(f"Tâche : prédire `{task_name}`")

    if "error" in task_result:
        st.warning(task_result["error"])
        continue

    st.write(f"**{task_result['n_samples']}** échantillons, **{task_result['n_classes']}** classes")

    # Metrics table
    metrics_data = []
    for clf_name in ["random_forest", "logistic_regression"]:
        if clf_name in task_result:
            m = task_result[clf_name]
            metrics_data.append({
                "Classifieur": clf_name.replace("_", " ").title(),
                "Accuracy": f"{m['accuracy']:.1%}",
                "F1 (weighted)": f"{m['f1_weighted']:.1%}",
            })
    st.table(pd.DataFrame(metrics_data))

    # Confusion matrix (RF)
    if "random_forest" in task_result:
        cm = np.array(task_result["random_forest"]["confusion_matrix"])
        classes = task_result.get("classes", [str(i) for i in range(len(cm))])

        # Truncate to top 15 classes for readability
        if len(classes) > 15:
            top_idx = np.argsort(cm.sum(axis=1))[-15:]
            cm = cm[np.ix_(top_idx, top_idx)]
            classes = [classes[i] for i in top_idx]

        fig_cm = px.imshow(cm, x=classes, y=classes, color_continuous_scale="Blues",
                           title=f"Matrice de confusion — Random Forest ({task_name})",
                           labels=dict(x="Prédit", y="Réel", color="Count"))
        st.plotly_chart(fig_cm, use_container_width=True)

    # Feature importance
    if "feature_importance" in task_result:
        imp = np.array(task_result["feature_importance"])
        top_n = 20
        top_idx = np.argsort(imp)[-top_n:]
        fig_imp = px.bar(x=imp[top_idx], y=[f"dim_{i}" for i in top_idx],
                         orientation="h", title=f"Top {top_n} dimensions (importance RF)",
                         labels=dict(x="Importance", y="Dimension"))
        fig_imp.update_layout(height=400)
        st.plotly_chart(fig_imp, use_container_width=True)
