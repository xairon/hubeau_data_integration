"""
Sensor de backfill automatique pour partitions manquantes
"""

from datetime import datetime
from typing import List
from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus
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
    """

    # Années à vérifier (2020 à année courante)
    current_year = datetime.now().year
    years_to_check = list(range(2020, current_year + 1))

    missing_partitions: List[str] = []

    # Vérifier chaque année
    for year in years_to_check:
        partition_key = str(year)

        # Récupérer les runs pour cette partition
        runs = context.instance.get_runs(
            filters={
                "job_name": "sync_all_yearly_data",
                "tags": {"dagster/partition": partition_key},
            },
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
