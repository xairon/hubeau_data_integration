"""Utilitaires communs pour le chargement DLT par batches.

Ces fonctions centralisent les patterns utilisés par nos assets Dagster
pour créer des pipelines DLT, exécuter des chargements batchés et extraire
des métriques sérialisables pour le monitoring.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional

import dlt


# ==========================================================================
# Modèles de données
# ==========================================================================


@dataclass
class BatchMetrics:
    """Métriques élémentaires produites pour chaque batch chargé."""

    batch_index: int
    input_rows: int
    rows_loaded: int
    write_disposition: str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "batch_index": self.batch_index,
            "input_rows": self.input_rows,
            "rows_loaded": self.rows_loaded,
            "write_disposition": self.write_disposition,
        }


# ==========================================================================
# Pipeline helpers
# ==========================================================================


def create_dlt_pipeline(
    pipeline_name: str,
    *,
    context: Optional[Any] = None,
    dataset_name: Optional[str] = None,
    progress: Optional[str] = "log",
) -> dlt.Pipeline:
    """Crée un pipeline DLT pour un run Dagster.

    Args:
        pipeline_name: Nom logique du pipeline.
        context: Contexte Dagster pour le logging (optionnel).
        dataset_name: Schéma de destination (défaut configurable via env).
        progress: Mode de reporting de DLT ("log", None, ...).

    Returns:
        Instance `dlt.Pipeline` configurée sur PostgreSQL.
    
    Note:
        Pipeline name is kept STABLE across Dagster runs to ensure DLT
        maintains consistent schema state. This is required for append
        mode to work correctly when multiple partitions write to the same table.
    """

    # Use stable pipeline name (no run_id suffix) to share DLT state across runs
    # This ensures schema versioning works correctly for append operations
    unique_name = pipeline_name

    dataset = dataset_name or os.getenv("DLT_DEFAULT_DATASET", "staging")

    from dlt.destinations import postgres

    destination = postgres(
        credentials={
            "database": os.getenv("PG_DB", "postgres"),
            "username": os.getenv("PG_USER", "postgres"),
            "password": os.getenv("PG_PASSWORD"),
            "host": os.getenv("PG_HOST", "localhost"),
            "port": int(os.getenv("PG_PORT", "5432")),
        }
    )

    return dlt.pipeline(
        pipeline_name=unique_name,
        destination=destination,
        dataset_name=dataset,
        progress=progress,
    )


# ==========================================================================
# Metrics helpers
# ==========================================================================


def extract_load_metrics(load_info: Any) -> Dict[str, Any]:
    """Extrait des métriques sérialisables depuis un objet LoadInfo DLT."""

    rows_loaded = 0
    jobs_count = 0

    try:
        for package in getattr(load_info, "load_packages", []) or []:
            for job in getattr(package, "jobs", []) or []:
                jobs_count += 1
                metrics = getattr(job, "metrics", None) or {}
                items = metrics.get("items", 0)
                if isinstance(items, (int, float)):
                    rows_loaded += int(items)
    except Exception:
        # On ne veut jamais casser un asset uniquement à cause d'un format
        # surprise renvoyé par DLT. Un warning sera loggé côté asset.
        pass

    return {
        "rows_loaded": rows_loaded,
        "jobs_count": jobs_count,
    }


# ==========================================================================
# Batch loading helper
# ==========================================================================


def _build_batch_resource(
    resource_name: str,
    batch: Iterable[Dict[str, Any]],
    write_disposition: str,
) -> dlt.resource:
    """Wrap d'un batch pour exécution unique via DLT."""

    materialized_batch = list(batch)

    @dlt.resource(name=resource_name, write_disposition=write_disposition)
    def _batch_resource() -> Iterator[Dict[str, Any]]:
        for row in materialized_batch:
            yield row

    return _batch_resource


def load_batches_from_iterator(
    *,
    pipeline: dlt.Pipeline,
    resource_name: str,
    batch_iterator: Iterable[Iterable[Dict[str, Any]]],
    context: Optional[Any] = None,
    initial_write_disposition: str = "replace",
    extra_metadata: Optional[Dict[str, Any]] = None,
    collect_per_batch_metrics: bool = True,
) -> Dict[str, Any]:
    """Charge un flux de batches séquentiellement et renvoie des métriques.

    Args:
        pipeline: Instance DLT prête à être exécutée.
        resource_name: Nom logique de la ressource/table cible.
        batch_iterator: Iterable produisant les batches.
        context: Contexte Dagster pour le logging (optionnel).
        initial_write_disposition: Disposition du premier batch.
        extra_metadata: Métadonnées additionnelles à inclure dans le retour.
        collect_per_batch_metrics: Active la collecte détaillée par batch.

    Returns:
        Dictionnaire prêt à être renvoyé par un asset Dagster.
    """

    total_rows = 0
    batches_loaded = 0
    per_batch: List[BatchMetrics] = []

    for batch_index, batch in enumerate(batch_iterator, start=1):
        materialized_batch = list(batch)
        if not materialized_batch:
            if context:
                context.log.debug(
                    "Batch %s vide pour %s, skip.", batch_index, resource_name
                )
            continue

        batches_loaded += 1
        write_disposition = (
            initial_write_disposition if batches_loaded == 1 else "append"
        )

        if context:
            context.log.info(
                "Batch %s (%s lignes) pour %s avec disposition %s",
                batch_index,
                len(materialized_batch),
                resource_name,
                write_disposition,
            )

        batch_resource = _build_batch_resource(
            resource_name,
            materialized_batch,
            write_disposition,
        )

        load_info = pipeline.run(batch_resource)
        load_metrics = extract_load_metrics(load_info)
        loaded_rows = int(load_metrics.get("rows_loaded", 0))
        if loaded_rows == 0 and materialized_batch:
            # Fallback: DLT ne fournit pas toujours le nombre de lignes chargées.
            # On utilise alors la taille du batch pour ne pas perdre l'information.
            loaded_rows = len(materialized_batch)
            if context:
                context.log.debug(
                    "DLT n'a pas remonté de métriques pour %s batch %s, fallback à %s lignes.",
                    resource_name,
                    batch_index,
                    loaded_rows,
                )

        total_rows += loaded_rows

        if context:
            context.log.info(
                "Batch %s chargé (%s lignes cumulées %s)",
                batch_index,
                loaded_rows,
                total_rows,
            )

        if collect_per_batch_metrics:
            per_batch.append(
                BatchMetrics(
                    batch_index=batch_index,
                    input_rows=len(materialized_batch),
                    rows_loaded=loaded_rows,
                    write_disposition=write_disposition,
                )
            )

    metadata: Dict[str, Any] = {
        "resource_name": resource_name,
        "rows_loaded": total_rows,
        "batches_loaded": batches_loaded,
    }

    if collect_per_batch_metrics and per_batch:
        metadata["batch_metrics"] = [metrics.as_dict() for metrics in per_batch]

    if extra_metadata:
        metadata.update({k: v for k, v in extra_metadata.items() if v is not None})

    if batches_loaded == 0 and context:
        context.log.warning("Aucun batch chargé pour %s", resource_name)

    return metadata


__all__ = [
    "BatchMetrics",
    "create_dlt_pipeline",
    "extract_load_metrics",
    "load_batches_from_iterator",
    "run_dlt_resource",
]


def run_dlt_resource(
    *,
    pipeline: dlt.Pipeline,
    resource: Any,
    context: Optional[Any] = None,
    table_name: Optional[str] = None,
    write_disposition: Optional[str] = None,
    extra_metadata: Optional[Dict[str, Any]] = None,
    **run_kwargs: Any,
) -> Dict[str, Any]:
    """Exécute `pipeline.run` et retourne des métriques sérialisables.

    Args:
        pipeline: Pipeline DLT configurée.
        resource: Ressource / generator DLT à charger.
        context: Contexte Dagster pour les logs (optionnel).
        table_name: Table de destination (optionnel).
        write_disposition: Mode d'écriture (replace/append/merge...).
        extra_metadata: Métadonnées additionnelles à enrichir dans la sortie.

    Returns:
        Dictionnaire contenant les métriques de chargement.
    """

    if table_name:
        run_kwargs.setdefault("table_name", table_name)
    if write_disposition:
        run_kwargs.setdefault("write_disposition", write_disposition)

    if context:
        context.log.debug(
            "Running DLT resource %s%s",
            getattr(resource, "name", str(resource)),
            f" -> {table_name}" if table_name else "",
        )

    load_info = pipeline.run(resource, **run_kwargs)
    metrics = extract_load_metrics(load_info)

    metadata: Dict[str, Any] = {
        "rows_loaded": metrics.get("rows_loaded", 0),
        "jobs_count": metrics.get("jobs_count", 0),
        "status": "success",
    }

    if table_name:
        metadata["table_name"] = table_name

    if extra_metadata:
        metadata.update({k: v for k, v in extra_metadata.items() if v is not None})

    return metadata


