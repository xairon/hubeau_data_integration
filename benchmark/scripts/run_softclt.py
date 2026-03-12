"""Run SoftCLT on unified piezo + hydro data.

Usage:
    cd benchmark
    python scripts/run_softclt.py                          # Default: 1k piezo + 1k hydro
    python scripts/run_softclt.py --piezo 500 --hydro 500   # Custom sizes
    python scripts/run_softclt.py --piezo 50 --hydro 50     # Quick test
"""

import argparse
import json
import sys
import time
import torch
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from embedding_benchmark.config import cfg
from embedding_benchmark.data_loader import load_unified_data
from embedding_benchmark.methods.softclt_method import run as run_softclt
from embedding_benchmark.evaluation import (
    run_full_evaluation, save_embeddings, save_window_embeddings,
)


def main():
    parser = argparse.ArgumentParser(description="SoftCLT unified benchmark (piezo + hydro)")
    parser.add_argument("--piezo", type=int, default=None, help=f"Piezo sample size (default: {cfg.piezo_sample_size})")
    parser.add_argument("--hydro", type=int, default=None, help=f"Hydro sample size (default: {cfg.hydro_sample_size})")
    parser.add_argument("--epochs", type=int, default=100, help="Training epochs (default: 100)")
    args = parser.parse_args()

    piezo_n = args.piezo or cfg.piezo_sample_size
    hydro_n = args.hydro or cfg.hydro_sample_size

    print(f"{'=' * 60}")
    print(f"SOFTCLT UNIFIED BENCHMARK — {piezo_n} piézo + {hydro_n} hydro")
    print(f"{'=' * 60}\n")

    # 1. Load data
    print("Chargement des données...")
    sample_df, series, dates = load_unified_data(piezo_n, hydro_n)

    # Build domain map
    domains_map = dict(zip(sample_df["station_id"], sample_df["domain"]))

    # 2. Train SoftCLT
    print(f"\n{'=' * 60}")
    print(f"TRAINING SOFTCLT ({len(series)} stations, {args.epochs} epochs)")
    print(f"{'=' * 60}\n")

    result = run_softclt(series, dates, domains=domains_map, n_epochs=args.epochs)

    # 3. Evaluate
    print(f"\n{'=' * 60}")
    print("ÉVALUATION")
    print(f"{'=' * 60}\n")

    metrics, labels = run_full_evaluation(
        result.station_embeddings,
        result.station_ids,
        result.domains,
        sample_df,
        window_embeddings=result.window_embeddings,
        method_name="SoftCLT_unified",
    )

    # 4. Save
    save_embeddings(
        result.station_embeddings, result.station_ids, result.domains,
        labels, "SoftCLT_unified", sample_df,
    )
    save_window_embeddings(result.window_embeddings, domains_map, "SoftCLT_unified")

    # Save model
    from embedding_benchmark.methods.softclt_method import _patch_softclt_loss
    model_path = cfg.models_dir / "softclt_unified.pt"
    # Model is not easily serializable (TS2Vec), save embeddings instead
    print(f"\n  Model artifacts saved to {cfg.results_dir}/")

    # 5. Summary
    print(f"\n{'=' * 60}")
    print("RÉSUMÉ")
    print(f"{'=' * 60}\n")

    for k, v in metrics.items():
        print(f"  {k}: {v}")

    print(f"\n  Temps total: {result.elapsed_seconds:.1f}s")
    print(f"\n  Pour lancer l'UI : cd benchmark && streamlit run app/app.py")


if __name__ == "__main__":
    main()
