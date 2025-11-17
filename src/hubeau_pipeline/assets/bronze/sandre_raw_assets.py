"""
Assets Dagster pour charger les référentiels SANDRE bruts
"""

# pyright: reportMissingImports=false

import os
from typing import Dict, Any, Optional, Tuple

from dagster import AssetExecutionContext, AssetOut, asset, multi_asset

from hubeau_pipeline.sources.sandre_raw_source import (
    SANDRE_RESOURCE_CONFIG,
    iter_sandre_batches,
)
from hubeau_pipeline.utils.dlt_batching import (
    create_dlt_pipeline,
    load_batches_from_iterator,
)


def _resolve_batch_size(context: AssetExecutionContext) -> Optional[int]:
    """Récupère la taille de batch souhaitée depuis l'environnement."""

    batch_size_env = os.getenv("SANDRE_BATCH_SIZE")
    if not batch_size_env:
        return None

    try:
        batch_size = int(batch_size_env)
        if batch_size <= 0:
            raise ValueError
        return batch_size
    except ValueError:
        context.log.warning(
            "Invalid SANDRE_BATCH_SIZE=%s. Falling back to API defaults.",
            batch_size_env,
        )
        return None


@multi_asset(
    outs={
        "sandre_parametres": AssetOut(description="Paramètres physico-chimiques SANDRE"),
        "sandre_unites": AssetOut(description="Unités de mesure SANDRE"),
        "sandre_methodes": AssetOut(description="Méthodes d'analyse SANDRE"),
        "sandre_supports": AssetOut(description="Supports de prélèvement SANDRE"),
        "sandre_fractions": AssetOut(description="Fractions analysées SANDRE"),
    },
    group_name="sandre_references",
    description="Référentiels d'analyse SANDRE"
)
def sandre_analysis_refs(context: AssetExecutionContext) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Charge les référentiels d'analyse SANDRE"""

    context.log.info("Loading SANDRE analysis references...")

    pipeline = create_dlt_pipeline(
        "sandre_analysis",
        context=context,
        dataset_name="sandre",
    )
    batch_size = _resolve_batch_size(context)

    resource_names = [
        "sandre_parametres",
        "sandre_unites",
        "sandre_methodes",
        "sandre_supports",
        "sandre_fractions",
    ]

    results = {}
    for resource_name in resource_names:
        results[resource_name] = load_batches_from_iterator(
            pipeline=pipeline,
            resource_name=resource_name,
            batch_iterator=iter_sandre_batches(
                resource_name,
                batch_size=batch_size,
            ),
            context=context,
            extra_metadata={
                "batch_size": batch_size,
                "endpoint": SANDRE_RESOURCE_CONFIG.get(resource_name, {}).get(
                    "endpoint"
                ),
            },
        )

    return (
        results.get("sandre_parametres", {}),
        results.get("sandre_unites", {}),
        results.get("sandre_methodes", {}),
        results.get("sandre_supports", {}),
        results.get("sandre_fractions", {})
    )


@multi_asset(
    outs={
        "sandre_communes": AssetOut(description="Communes françaises SANDRE"),
        "sandre_departements": AssetOut(description="Départements SANDRE"),
        "sandre_regions": AssetOut(description="Régions SANDRE"),
        "sandre_bassins": AssetOut(description="Bassins versants SANDRE"),
    },
    group_name="sandre_territories",
    description="Référentiels territoriaux SANDRE"
)
def sandre_territorial_refs(context: AssetExecutionContext) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Charge les référentiels territoriaux SANDRE"""

    context.log.info("Loading SANDRE territorial references...")

    pipeline = create_dlt_pipeline(
        "sandre_territorial",
        context=context,
        dataset_name="sandre",
    )
    batch_size = _resolve_batch_size(context)

    resource_names = [
        "sandre_communes",
        "sandre_departements",
        "sandre_regions",
        "sandre_bassins",
    ]

    results = {}
    for resource_name in resource_names:
        results[resource_name] = load_batches_from_iterator(
            pipeline=pipeline,
            resource_name=resource_name,
            batch_iterator=iter_sandre_batches(
                resource_name,
                batch_size=batch_size,
            ),
            context=context,
            extra_metadata={
                "batch_size": batch_size,
                "endpoint": SANDRE_RESOURCE_CONFIG.get(resource_name, {}).get(
                    "endpoint"
                ),
            },
        )

    return (
        results.get("sandre_communes", {}),
        results.get("sandre_departements", {}),
        results.get("sandre_regions", {}),
        results.get("sandre_bassins", {})
    )


@multi_asset(
    outs={
        "sandre_masses_eau_sout": AssetOut(description="Masses d'eau souterraines SANDRE"),
        "sandre_masses_eau_surf": AssetOut(description="Masses d'eau de surface SANDRE"),
        "sandre_cours_eau": AssetOut(description="Cours d'eau SANDRE"),
        "sandre_milieux": AssetOut(description="Milieux de prélèvement SANDRE"),
    },
    group_name="sandre_hydro",
    description="Référentiels hydrographiques SANDRE"
)
def sandre_hydro_refs(context: AssetExecutionContext) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Charge les référentiels hydrographiques SANDRE"""

    context.log.info("Loading SANDRE hydrographic references...")

    pipeline = create_dlt_pipeline(
        "sandre_hydro",
        context=context,
        dataset_name="sandre",
    )
    batch_size = _resolve_batch_size(context)

    resource_names = [
        "sandre_masses_eau_sout",
        "sandre_masses_eau_surf",
        "sandre_cours_eau",
        "sandre_milieux",
    ]

    results = {}
    for resource_name in resource_names:
        results[resource_name] = load_batches_from_iterator(
            pipeline=pipeline,
            resource_name=resource_name,
            batch_iterator=iter_sandre_batches(
                resource_name,
                batch_size=batch_size,
            ),
            context=context,
            extra_metadata={
                "batch_size": batch_size,
                "endpoint": SANDRE_RESOURCE_CONFIG.get(resource_name, {}).get(
                    "endpoint"
                ),
            },
        )

    return (
        results.get("sandre_masses_eau_sout", {}),
        results.get("sandre_masses_eau_surf", {}),
        results.get("sandre_cours_eau", {}),
        results.get("sandre_milieux", {})
    )


@asset(
    group_name="sandre_organizations",
    description="Intervenants/organismes SANDRE"
)
def sandre_intervenants(context: AssetExecutionContext) -> Dict[str, Any]:
    """Charge les intervenants SANDRE (organismes, laboratoires)"""

    context.log.info("Loading SANDRE intervenants...")

    pipeline = create_dlt_pipeline(
        "sandre_orgs",
        context=context,
        dataset_name="sandre",
    )
    batch_size = _resolve_batch_size(context)

    metrics = load_batches_from_iterator(
        pipeline=pipeline,
        resource_name="sandre_intervenants",
        batch_iterator=iter_sandre_batches(
            "sandre_intervenants",
            batch_size=batch_size,
        ),
        context=context,
        extra_metadata={
            "batch_size": batch_size,
            "endpoint": SANDRE_RESOURCE_CONFIG.get("sandre_intervenants", {}).get(
                "endpoint"
            ),
        },
    )

    context.log.info(
        "Loaded %s intervenants across %s batches",
        metrics.get("rows_loaded", 0),
        metrics.get("batches_loaded", 0),
    )

    return metrics