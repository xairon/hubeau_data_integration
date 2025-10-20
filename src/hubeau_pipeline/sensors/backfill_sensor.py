"""
Sensor de backfill automatique pour partitions manquantes
"""

import os
from datetime import datetime
from typing import List
from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus, RunsFilter
from ..jobs import sync_all_yearly_data


@sensor(
    job=sync_all_yearly_data,
    minimum_interval_seconds=3600,  # Vérifie toutes les heures
    default_status=DefaultSensorStatus.RUNNING,
    description="Détecte et backfill automatiquement les partitions annuelles manquantes",
)
def backfill_missing_partitions_sensor(context: SensorEvaluationContext):
    """
    Détecte les partitions annuelles manquantes et crée des runs pour les backfiller.

    Logique:
    - Vérifie les années 2020 à année courante
    - Pour chaque année, vérifie si au moins un run a réussi
    - Si aucun run réussi, crée un RunRequest pour cette partition
    - Limite à 3 backfills par exécution pour éviter surcharge
    - Ne backfill pas automatiquement lors d'une nouvelle installation
    """

    # Vérifier si c'est une nouvelle installation (aucun run historique)
    all_historical_runs = context.instance.get_runs(
        filters=RunsFilter(job_name="sync_all_yearly_data"),
        limit=10  # On vérifie les 10 derniers runs
    )

    # Si aucun run n'existe et qu'on ne force pas le backfill initial
    if not all_historical_runs and not os.getenv("FORCE_INITIAL_BACKFILL", "").lower() == "true":
        context.log.info(
            "🆕 Nouvelle installation détectée - pas de backfill automatique. "
            "Pour forcer le backfill initial, définir FORCE_INITIAL_BACKFILL=true "
            "ou lancer manuellement un job pour une année spécifique."
        )
        return

    # Vérifier aussi qu'au moins un run est terminé (SUCCESS ou FAILURE)
    # pour éviter de backfiller pendant que les premiers runs sont encore en cours
    completed_runs = [run for run in all_historical_runs if run.status.value in ["SUCCESS", "FAILURE"]]
    if all_historical_runs and not completed_runs:
        context.log.info("⏳ Des runs sont en cours, attente avant de vérifier les partitions manquantes")
        return

    # Si on a moins de 3 runs réussis, on est probablement encore en phase d'initialisation
    successful_runs = [run for run in all_historical_runs if run.status.value == "SUCCESS"]
    if len(successful_runs) < 3 and not os.getenv("FORCE_INITIAL_BACKFILL", "").lower() == "true":
        context.log.info(
            f"🚀 Phase d'initialisation ({len(successful_runs)} runs réussis). "
            "Le backfill automatique démarrera après 3 runs réussis."
        )
        return

    # Années à vérifier (2020 à année courante)
    current_year = datetime.now().year
    years_to_check = list(range(2020, current_year + 1))

    missing_partitions: List[str] = []

    # Vérifier chaque année
    for year in years_to_check:
        partition_key = str(year)

        # Récupérer les runs pour cette partition
        runs = context.instance.get_runs(
            filters=RunsFilter(
                job_name="sync_all_yearly_data",
                tags={"dagster/partition": partition_key},
            ),
            limit=10,
        )

        # Vérifier si au moins un run a réussi
        has_successful_run = any(run.status.value == "SUCCESS" for run in runs)

        if not has_successful_run:
            missing_partitions.append(partition_key)
            context.log.info(f"📋 Partition manquante détectée: {partition_key}")

    # Limiter à 3 backfills par exécution
    partitions_to_backfill = missing_partitions[:3]

    if not partitions_to_backfill:
        context.log.info("✅ Toutes les partitions sont à jour")
        return

    # Créer RunRequests pour les partitions manquantes
    for partition_key in partitions_to_backfill:
        yield RunRequest(
            run_key=f"backfill_{partition_key}_{datetime.now().isoformat()}",
            partition_key=partition_key,
            tags={
                "backfill": "auto",
                "partition": partition_key,
            },
        )
        context.log.info(f"🔄 Backfill planifié pour partition: {partition_key}")

    # Log récapitulatif
    if len(missing_partitions) > 3:
        context.log.warning(
            f"⚠️ {len(missing_partitions) - 3} partitions additionnelles manquantes "
            f"seront backfillées lors des prochaines exécutions"
        )
