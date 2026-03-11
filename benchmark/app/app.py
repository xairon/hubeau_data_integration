"""
Démonstrateur Streamlit — Benchmark Embeddings Hydrologiques.

Lancement : cd benchmark && streamlit run app/app.py
Prérequis : avoir exécuté run_all.py pour générer results/
"""

import streamlit as st
from pathlib import Path
import sys

# Ajouter src/ au path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

st.set_page_config(
    page_title="Benchmark Embeddings Hydro",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("💧 Benchmark Embeddings — Séries Temporelles Hydrologiques")

st.markdown("""
### Objectif

Comparer **5 méthodes d'embedding** sur ~300 stations piézométriques :

| # | Méthode | Type |
|---|---------|------|
| 1 | **tsfresh** | Features classiques (baseline) |
| 2 | **MOMENT** | Foundation model zero-shot (univariate) |
| 3 | **Chronos-2** | Foundation model zero-shot (multivariate) |
| 4 | **TS2Vec** | Contrastif hiérarchique (entraîné) |
| 5 | **SoftCLT** | Contrastif soft assignments (entraîné) |

### Navigation

- **Comparaison** : tableau récapitulatif + radar chart
- **Exploration embeddings** : UMAP interactif coloré par cluster/nature_eh
- **Similarité stations** : recherche des k plus proches voisins
""")

# Vérifier que les résultats existent
results_dir = Path(__file__).parent.parent / "results"
metrics_dir = results_dir / "metrics"
embeddings_dir = results_dir / "embeddings"

if not metrics_dir.exists() or not list(metrics_dir.glob("*.json")):
    st.warning("⚠️ Aucun résultat trouvé. Exécuter d'abord le benchmark :")
    st.code("cd benchmark && python scripts/run_all.py", language="bash")
else:
    n_methods = len(list(metrics_dir.glob("*.json"))) - (1 if (metrics_dir / "summary.json").exists() else 0)
    n_embeddings = len(list(embeddings_dir.glob("*.parquet")))
    st.success(f"✅ {n_methods} méthodes évaluées, {n_embeddings} fichiers embeddings disponibles.")
