"""
Schedules Dagster - Planification automatique des jobs CSV

NOTE: Schedules temporairement désactivés pendant migration vers assets partitionnés
"""

# TODO: Migrer les schedules pour utiliser AutoMaterializePolicy avec assets partitionnés
# Référence: https://docs.dagster.io/concepts/partitions-schedules-sensors/auto-materialize-policies

# ====================================
# SCHEDULES ACTIFS
# ====================================

# Schedules désactivés temporairement - migration en cours
all_schedules = []

__all__ = ["all_schedules"]
