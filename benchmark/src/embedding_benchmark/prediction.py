"""Prédiction downstream avec embeddings comme features."""

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score, confusion_matrix
from sklearn.preprocessing import LabelEncoder


def run_classification(embeddings: np.ndarray, labels: list[str],
                       task_name: str, test_size: float = 0.2,
                       seed: int = 42) -> dict:
    """Run RF + LogReg classification on embeddings. Returns metrics dict."""
    le = LabelEncoder()
    y = le.fit_transform(labels)
    classes = le.classes_

    # Filter classes with too few samples for stratified split
    class_counts = np.bincount(y)
    valid_classes = np.where(class_counts >= 2)[0]
    if len(valid_classes) < 2:
        return {"task": task_name, "error": "Too few classes with ≥2 samples"}

    valid_mask = np.isin(y, valid_classes)
    X = embeddings[valid_mask]
    y = y[valid_mask]
    classes = classes[valid_classes]

    # Re-encode after filtering
    le2 = LabelEncoder()
    y = le2.fit_transform([classes[yi] for yi in y] if len(valid_classes) < len(le.classes_) else le.inverse_transform(y))
    classes = le2.classes_

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=seed, stratify=y
    )

    results = {"task": task_name, "n_samples": len(X), "n_classes": len(classes),
               "classes": classes.tolist()}

    for name, clf in [("random_forest", RandomForestClassifier(n_estimators=100, random_state=seed)),
                      ("logistic_regression", LogisticRegression(max_iter=1000, random_state=seed))]:
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred, average="weighted")
        cm = confusion_matrix(y_test, y_pred)

        results[name] = {
            "accuracy": round(acc, 4),
            "f1_weighted": round(f1, 4),
            "confusion_matrix": cm.tolist(),
        }

        # Feature importance (RF only)
        if name == "random_forest":
            results["feature_importance"] = clf.feature_importances_.tolist()

    return results


def run_all_predictions(embeddings: np.ndarray, station_ids: list[str],
                        domains: list[str], meta_df: pd.DataFrame) -> list[dict]:
    """Run all downstream prediction tasks."""
    results = []
    meta_map = meta_df.set_index("station_id")

    # Task 1: Predict domain (piezo vs hydro) — binary
    results.append(run_classification(embeddings, domains, "domain"))

    # Task 2: Predict nature_eh (piezo only)
    piezo_mask = np.array(domains) == "piezo"
    if piezo_mask.sum() > 10 and "nature_eh" in meta_df.columns:
        piezo_ids = [s for s, d in zip(station_ids, domains) if d == "piezo"]
        nature_labels = [meta_map.loc[s, "nature_eh"] if s in meta_map.index else "unknown"
                         for s in piezo_ids]
        # Filter out unknown/NaN
        valid = [(e, l) for e, l, m in zip(embeddings[piezo_mask], nature_labels, [True]*piezo_mask.sum())
                 if l != "unknown" and pd.notna(l)]
        if len(valid) > 10:
            X = np.stack([v[0] for v in valid])
            y = [v[1] for v in valid]
            results.append(run_classification(X, y, "nature_eh"))

    # Task 3: Predict region
    if "code_region" in meta_df.columns:
        region_labels = [meta_map.loc[s, "code_region"] if s in meta_map.index else "unknown"
                         for s in station_ids]
        valid = [(e, l) for e, l in zip(embeddings, region_labels)
                 if l != "unknown" and pd.notna(l)]
        if len(valid) > 10:
            X = np.stack([v[0] for v in valid])
            y = [v[1] for v in valid]
            results.append(run_classification(X, y, "region"))

    return results
