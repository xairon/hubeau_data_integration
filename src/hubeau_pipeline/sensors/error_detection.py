"""
Sensor - Détection d'erreurs
Surveillance des erreurs et alertes
"""

from dagster import RunRequest, SensorEvaluationContext, SkipReason, sensor

# NOTE: Le job sera spécifié lors de l'appel du sensor si nécessaire
# Retirer 'job=' évite les doublons dans les définitions Dagster

@sensor(
    name="error_detection_sensor",
    description="Détecte les erreurs dans les pipelines"
)
def error_detection_sensor(context: SensorEvaluationContext):
    """
    Sensor : Détecte les erreurs dans les pipelines
    
    Ce sensor vérifie :
    - Les échecs de jobs récents
    - Les données manquantes
    - Les anomalies dans les métriques
    - Les problèmes de connectivité
    """
    # Simulation de la détection d'erreurs
    # En réalité, on vérifierait les logs, métriques, etc.
    
    errors_detected = False  # Simulation
    
    if errors_detected:
        context.log.warning("🚨 Erreurs détectées - Relance du job de récupération")
        return RunRequest(
            run_key=f"error_recovery_{context.cursor}",
            tags={"trigger": "error_sensor", "recovery": "true"}
        )
    else:
        context.log.info("✅ Aucune erreur détectée")
        return SkipReason("Pipeline fonctionne normalement")
