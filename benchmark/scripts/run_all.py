"""
Script CLI pour exécuter le benchmark complet.

Usage:
    cd benchmark
    python scripts/run_all.py                     # Toutes les méthodes
    python scripts/run_all.py --methods tsfresh MOMENT  # Méthodes spécifiques
    python scripts/run_all.py --sample-size 50    # Petit échantillon (debug)
"""

import argparse
import json
import sys
import time
from pathlib import Path

# Ajouter src/ au path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from embedding_benchmark.config import cfg
from embedding_benchmark.data_loader import get_eligible_stations, sample_stations, load_series
from embedding_benchmark.evaluation import run_full_evaluation, save_embeddings, cluster_hdbscan


METHODS = {
    "tsfresh": "embedding_benchmark.methods.tsfresh_method",
    "MOMENT": "embedding_benchmark.methods.moment_method",
    "Chronos-2": "embedding_benchmark.methods.chronos2_method",
    "TS2Vec": "embedding_benchmark.methods.ts2vec_method",
    "SoftCLT": "embedding_benchmark.methods.softclt_method",
}


def run_method(name: str, series, dates):
    """Importe et exécute une méthode par son nom."""
    import importlib
    module = importlib.import_module(METHODS[name])
    return module.run(series, dates)


def main():
    parser = argparse.ArgumentParser(description="Benchmark embeddings séries temporelles")
    parser.add_argument("--methods", nargs="+", default=list(METHODS.keys()),
                        choices=list(METHODS.keys()), help="Méthodes à exécuter")
    parser.add_argument("--sample-size", type=int, default=None,
                        help=f"Taille échantillon (défaut: {cfg.sample_size})")
    args = parser.parse_args()

    if args.sample_size:
        cfg.sample_size = args.sample_size

    print(f"{'='*60}")
    print(f"BENCHMARK EMBEDDINGS — {len(args.methods)} méthodes, {cfg.sample_size} stations")
    print(f"{'='*60}\n")

    # 1. Charger les données
    print("Chargement des stations éligibles...")
    eligible = get_eligible_stations()
    print(f"  {len(eligible)} stations éligibles")

    sample = sample_stations(eligible)
    print(f"  {len(sample)} stations échantillonnées")

    print("Chargement des séries temporelles...")
    series, dates = load_series(sample["code_bss"].tolist())
    print(f"  {len(series)} séries chargées\n")

    # 2. Exécuter chaque méthode
    all_results = []
    for method_name in args.methods:
        print(f"\n{'='*60}")
        print(f"MÉTHODE : {method_name}")
        print(f"{'='*60}")

        try:
            result = run_method(method_name, series, dates)

            # Évaluation
            labels = cluster_hdbscan(result.station_embeddings)
            metrics = run_full_evaluation(
                result.station_embeddings,
                result.station_ids,
                sample,
                window_embeddings=result.window_embeddings,
                method_name=result.method_name,
            )
            metrics["time_seconds"] = round(result.elapsed_seconds, 1)
            all_results.append(metrics)

            # Sauvegarder embeddings en parquet
            save_embeddings(
                result.station_embeddings,
                result.station_ids,
                labels,
                result.method_name,
                sample,
            )

            print(f"\n  Résultats {result.method_name}:")
            for k, v in metrics.items():
                if k != "method":
                    print(f"    {k}: {v}")

        except Exception as e:
            print(f"\n  ERREUR {method_name}: {e}")
            import traceback
            traceback.print_exc()

    # 3. Résumé
    if all_results:
        print(f"\n\n{'='*60}")
        print("RÉSUMÉ COMPARATIF")
        print(f"{'='*60}\n")

        import pandas as pd
        df = pd.DataFrame(all_results)
        print(df.to_string(index=False))

        # Sauvegarder résumé
        summary_path = cfg.metrics_dir / "summary.json"
        with open(summary_path, "w") as f:
            json.dump(all_results, f, indent=2)
        print(f"\nRésultats sauvegardés dans {summary_path}")
        print(f"Embeddings sauvegardés dans {cfg.embeddings_dir}/")
        print(f"\nPour lancer l'UI : cd benchmark && streamlit run app/app.py")


if __name__ == "__main__":
    main()
