"""SoftCLT Embedding Platform — Piezo + Hydro."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import streamlit as st
from embedding_benchmark.ui.components import has_results, load_metrics

st.set_page_config(page_title="SoftCLT Embeddings", page_icon="🌊", layout="wide")

st.title("SoftCLT Embedding Platform")
st.markdown("Exploration des embeddings de séries temporelles hydrologiques (piézométrie + hydrométrie)")

if not has_results():
    st.error("Aucun résultat trouvé. Lancez d'abord le benchmark :")
    st.code("cd benchmark && python scripts/run_softclt.py --piezo 50 --hydro 50")
    st.stop()

metrics = load_metrics()
if metrics:
    cols = st.columns(4)
    cols[0].metric("Stations", metrics.get("n_stations", "?"))
    cols[1].metric("Clusters", metrics.get("n_clusters", "?"))
    cols[2].metric("Silhouette", metrics.get("silhouette", "?"))
    cols[3].metric("Stabilité temporelle", metrics.get("temporal_stability", "?"))

    st.markdown("---")
    st.markdown("Naviguez dans les pages ci-dessous pour explorer les embeddings.")
