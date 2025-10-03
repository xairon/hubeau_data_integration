"""
Sensor - Fraîcheur des données
Surveillance de la fraîcheur des données Hub'Eau
"""

from datetime import datetime, timedelta

from dagster import RunRequest, SensorEvaluationContext, SkipReason, sensor

from hubeau_pipeline.jobs import sync_realtime_data

@sensor(
    job=sync_realtime_data,
    name="hubeau_freshness_sensor",
    description="Surveille la fraîcheur des données Hub'Eau"
)
def hubeau_freshness_sensor(context: SensorEvaluationContext):
    """
    Sensor : Surveille la fraîcheur des données
    
    Ce sensor vérifie :
    - Si les données sont à jour
    - Si les APIs Hub'Eau sont disponibles
    - Si des nouvelles données sont disponibles
    """
    # Par défaut on suit l'heure du dernier déclenchement pour éviter les doublons
    last_tick_raw = context.cursor
    if last_tick_raw:
        last_tick = datetime.fromisoformat(last_tick_raw)
    else:
        # Première exécution : on remonte 2 heures en arrière pour balayer la fenêtre temps réel
        last_tick = datetime.utcnow() - timedelta(hours=2)

    next_window_start = last_tick + timedelta(minutes=30)
    now = datetime.utcnow()

    if next_window_start <= now:
        context.log.info("🔄 Nouvelles données Hub'Eau détectées - Lancement du job temps réel")
        context.update_cursor(now.isoformat())
        return RunRequest(
            run_key=f"hubeau_freshness_{now.isoformat()}",
            tags={"trigger": "freshness_sensor", "source": "hubeau"}
        )

    context.log.info("⏸️ Fenêtre temps réel déjà couverte - Job non lancé")
    return SkipReason("Aucune nouvelle fenêtre de données temps réel détectée")
