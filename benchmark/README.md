# Benchmark Embeddings — Séries Temporelles Hydrologiques

Comparaison de 5 méthodes d'embedding pour les chroniques piézométriques.

## Méthodes

| # | Méthode | Type | GPU | Training |
|---|---------|------|-----|----------|
| 1 | tsfresh | Features classiques | Non | Non |
| 2 | MOMENT | Foundation model (zero-shot) | Optionnel | Non |
| 3 | Chronos-2 | Foundation model (zero-shot) | Optionnel | Non |
| 4 | TS2Vec | Contrastif hiérarchique | Optionnel | Oui |
| 5 | SoftCLT | Contrastif soft (ICLR 2024) | Optionnel | Oui |

## Installation

```bash
cd benchmark
python -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env
# Éditer .env avec les credentials PostgreSQL
```

## Exécution

```bash
# Toutes les méthodes
python scripts/run_all.py

# Méthodes spécifiques
python scripts/run_all.py --methods tsfresh MOMENT

# Petit échantillon (debug)
python scripts/run_all.py --methods tsfresh --sample-size 10
```

## UI Streamlit

```bash
streamlit run app/app.py
```

3 pages :
- **Comparaison** : tableau, score composite (pondération interactive), radar chart
- **Exploration** : UMAP interactif coloré par cluster/nature_eh/milieu_eh
- **Similarité** : recherche kNN, cohérence, UMAP avec highlight

## Prérequis

- Python 3.11+
- Accès PostgreSQL à l'entrepôt Hub'Eau (lecture seule sur `gold.*`)
- ~4 GB RAM (300 stations, CPU)
- GPU optionnel (accélère MOMENT, Chronos-2, TS2Vec, SoftCLT)
