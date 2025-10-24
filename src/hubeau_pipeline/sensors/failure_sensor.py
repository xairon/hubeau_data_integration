"""
Sensor d'alerting sur échecs de pipeline
"""

from datetime import datetime, timedelta
from dagster import (
    sensor,
    SensorEvaluationContext,
    DefaultSensorStatus,
    RunsFilter,
    DagsterRunStatus,
)


@sensor(
    minimum_interval_seconds=300,  # Vérifie toutes les 5 minutes
    default_status=DefaultSensorStatus.RUNNING,
    description="Détecte les échecs de pipeline et log des alertes",
)
def pipeline_failure_alert_sensor(context: SensorEvaluationContext):
    """
    Détecte les runs échoués récents et log des alertes.

    TODO: Intégrer avec Slack/Email pour vraies notifications
    """

    # Chercher les runs échoués dans les dernières 24h
    since = datetime.now() - timedelta(hours=24)

    failed_run_records = context.instance.get_run_records(
        filters=RunsFilter(
            statuses=[DagsterRunStatus.FAILURE],
            updated_after=since,
        ),
        limit=10,
    )

    if not failed_run_records:
        context.log.debug("✅ Aucun échec détecté dans les dernières 24h")
        return

    # Log chaque échec
    for run_record in failed_run_records:
        run = run_record.dagster_run
        job_name = run.job_name
        run_id = run.run_id
        # Use RunRecord.create_timestamp which is always available
        created_timestamp = run_record.create_timestamp

        context.log.error(
            f"❌ ÉCHEC DÉTECTÉ - Job: {job_name}, Run ID: {run_id}, "
            f"Créé le: {datetime.fromtimestamp(created_timestamp) if created_timestamp else 'unknown'}"
        )

        # TODO: Envoyer notification Slack
        # send_slack_notification(
        #     channel="#data-alerts",
        #     message=f"🚨 Pipeline échoué: {job_name} (Run: {run_id})"
        # )

        # TODO: Envoyer email
        # send_email_alert(
        #     to="data-team@example.com",
        #     subject=f"Pipeline Failure: {job_name}",
        #     body=f"Run {run_id} failed. Check Dagster UI for details."
        # )

    # Log récapitulatif
    context.log.warning(
        f"⚠️ TOTAL: {len(failed_run_records)} échec(s) détecté(s) dans les dernières 24h"
    )


@sensor(
    minimum_interval_seconds=1800,  # Vérifie toutes les 30 minutes
    default_status=DefaultSensorStatus.STOPPED,  # Désactivé par défaut
    description="Détecte les runs qui tournent trop longtemps (> 2h)",
)
def long_running_pipeline_sensor(context: SensorEvaluationContext):
    """
    Détecte les runs qui tournent depuis plus de 2 heures.

    Utile pour détecter les pipelines bloqués.
    """

    # Runs en cours
    running_run_records = context.instance.get_run_records(
        filters=RunsFilter(
            statuses=[DagsterRunStatus.STARTED],
        ),
        limit=20,
    )

    long_running_threshold = timedelta(hours=2)
    now = datetime.now()

    for run_record in running_run_records:
        run = run_record.dagster_run
        if run_record.start_time:
            duration = now - datetime.fromtimestamp(run_record.start_time)

            if duration > long_running_threshold:
                context.log.warning(
                    f"⏱️ RUN LONG - Job: {run.job_name}, Run ID: {run.run_id}, "
                    f"Durée: {duration}"
                )

                # TODO: Envoyer notification
                # send_slack_notification(
                #     channel="#data-alerts",
                #     message=f"⏱️ Pipeline long: {run.job_name} tourne depuis {duration}"
                # )


@sensor(
    minimum_interval_seconds=3600,  # Vérifie toutes les heures
    default_status=DefaultSensorStatus.STOPPED,  # Désactivé par défaut
    description="Détecte les échecs répétés (>= 3 fois) sur même partition",
)
def repeated_failure_sensor(context: SensorEvaluationContext):
    """
    Détecte les partitions qui échouent de manière répétée.

    Utile pour identifier les données problématiques.
    """

    # Chercher les runs échoués dans les derniers 7 jours
    since = datetime.now() - timedelta(days=7)

    failed_run_records = context.instance.get_run_records(
        filters=RunsFilter(
            statuses=[DagsterRunStatus.FAILURE],
            updated_after=since,
        ),
        limit=100,
    )

    # Grouper par job + partition
    failures_by_partition = {}

    for run_record in failed_run_records:
        run = run_record.dagster_run
        job_name = run.job_name
        partition_key = run.tags.get("dagster/partition", "no_partition")
        key = f"{job_name}:{partition_key}"

        if key not in failures_by_partition:
            failures_by_partition[key] = []

        failures_by_partition[key].append(run_record)

    # Identifier partitions avec >= 3 échecs
    problematic_partitions = {
        key: runs for key, runs in failures_by_partition.items() if len(runs) >= 3
    }

    if not problematic_partitions:
        context.log.debug("✅ Aucune partition avec échecs répétés")
        return

    # Log partitions problématiques
    for key, runs in problematic_partitions.items():
        job_name, partition_key = key.split(":", 1)

        context.log.error(
            f"🔴 PARTITION PROBLÉMATIQUE - Job: {job_name}, "
            f"Partition: {partition_key}, Échecs: {len(runs)}"
        )

        # TODO: Créer issue ou ticket automatique
        # create_jira_ticket(
        #     title=f"Partition failing repeatedly: {job_name}:{partition_key}",
        #     description=f"{len(runs)} failures in last 7 days"
        # )

    context.log.warning(
        f"⚠️ TOTAL: {len(problematic_partitions)} partition(s) avec échecs répétés"
    )
