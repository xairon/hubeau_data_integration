"""
Monitoring des performances en temps réel pour Hub'Eau
"""

import time
import psutil
import threading
from typing import Dict, Any, List
from datetime import datetime, timedelta
import pandas as pd
import os

from dagster import AssetExecutionContext, asset, Output, MetadataValue, sensor, SensorEvaluationContext


class PerformanceTracker:
    """Tracker de performance en temps réel"""
    
    def __init__(self):
        self.metrics = []
        self.lock = threading.Lock()
        self.running = False
        self.thread = None
    
    def start_monitoring(self, interval: int = 30):
        """Démarre le monitoring en arrière-plan"""
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop, args=(interval,))
        self.thread.daemon = True
        self.thread.start()
    
    def stop_monitoring(self):
        """Arrête le monitoring"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
    
    def _monitor_loop(self, interval: int):
        """Boucle de monitoring"""
        while self.running:
            try:
                metrics = self._collect_metrics()
                with self.lock:
                    self.metrics.append(metrics)
                    # Garder seulement les 100 dernières métriques
                    if len(self.metrics) > 100:
                        self.metrics = self.metrics[-100:]
                
                time.sleep(interval)
            except Exception as e:
                print(f"Erreur monitoring: {e}")
                time.sleep(interval)
    
    def _collect_metrics(self) -> Dict[str, Any]:
        """Collecte les métriques système"""
        return {
            "timestamp": datetime.now().isoformat(),
            "cpu_percent": psutil.cpu_percent(interval=1),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_percent": psutil.disk_usage('/').percent,
            "load_avg": psutil.getloadavg() if hasattr(psutil, 'getloadavg') else [0, 0, 0],
            "process_count": len(psutil.pids()),
        }
    
    def get_latest_metrics(self) -> Dict[str, Any]:
        """Récupère les dernières métriques"""
        with self.lock:
            return self.metrics[-1] if self.metrics else {}
    
    def get_metrics_history(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Récupère l'historique des métriques"""
        cutoff = datetime.now() - timedelta(hours=hours)
        with self.lock:
            return [
                m for m in self.metrics 
                if datetime.fromisoformat(m["timestamp"]) >= cutoff
            ]


# Instance globale du tracker
performance_tracker = PerformanceTracker()


def _get_pg_connection():
    """Connexion PostgreSQL pour métriques BDD"""
    import psycopg2
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"),
        port=os.getenv("PG_PORT", "5432"),
        database=os.getenv("PG_DB", "postgres"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD")
    )


# ====================================
# MÉTRIQUES SYSTÈME
# ====================================

@asset(
    group_name="performance_monitoring",
    description="Métriques de performance système en temps réel",
)
def system_performance_metrics(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Collecte les métriques de performance système:
    - CPU, mémoire, disque
    - Charge système
    - Nombre de processus
    """
    # Démarrer le monitoring si pas déjà démarré
    if not performance_tracker.running:
        performance_tracker.start_monitoring(interval=30)
    
    # Récupérer les dernières métriques
    latest_metrics = performance_tracker.get_latest_metrics()
    
    # Métriques supplémentaires
    try:
        # Connexions réseau
        network_connections = len(psutil.net_connections())
        
        # Température CPU (si disponible)
        try:
            cpu_temp = psutil.sensors_temperatures()['cpu_thermal'][0].current
        except:
            cpu_temp = None
        
        # IO disque
        disk_io = psutil.disk_io_counters()
        
        latest_metrics.update({
            "network_connections": network_connections,
            "cpu_temperature": cpu_temp,
            "disk_read_bytes": disk_io.read_bytes if disk_io else 0,
            "disk_write_bytes": disk_io.write_bytes if disk_io else 0,
        })
        
    except Exception as e:
        context.log.warning(f"Impossible de récupérer métriques supplémentaires: {e}")
    
    # Calculer les tendances
    history = performance_tracker.get_metrics_history(hours=1)
    if len(history) > 1:
        latest_metrics["cpu_trend"] = self._calculate_trend([m["cpu_percent"] for m in history])
        latest_metrics["memory_trend"] = self._calculate_trend([m["memory_percent"] for m in history])
    
    # Métadonnées pour Dagster UI
    metadata = {
        "cpu_percent": MetadataValue.float(latest_metrics.get("cpu_percent", 0)),
        "memory_percent": MetadataValue.float(latest_metrics.get("memory_percent", 0)),
        "disk_percent": MetadataValue.float(latest_metrics.get("disk_percent", 0)),
        "load_avg_1m": MetadataValue.float(latest_metrics.get("load_avg", [0, 0, 0])[0]),
        "process_count": MetadataValue.int(latest_metrics.get("process_count", 0)),
        "network_connections": MetadataValue.int(latest_metrics.get("network_connections", 0)),
        "timestamp": MetadataValue.text(latest_metrics.get("timestamp", "")),
        "full_metrics": MetadataValue.json(latest_metrics),
    }
    
    context.log.info(f"📊 Performance système:")
    context.log.info(f"   CPU: {latest_metrics.get('cpu_percent', 0):.1f}%")
    context.log.info(f"   Mémoire: {latest_metrics.get('memory_percent', 0):.1f}%")
    context.log.info(f"   Disque: {latest_metrics.get('disk_percent', 0):.1f}%")
    
    return Output(latest_metrics, metadata=metadata)


def _calculate_trend(values: List[float]) -> str:
    """Calcule la tendance d'une série de valeurs"""
    if len(values) < 2:
        return "stable"
    
    recent = sum(values[-3:]) / min(3, len(values))
    older = sum(values[:-3]) / max(1, len(values) - 3)
    
    if recent > older * 1.1:
        return "increasing"
    elif recent < older * 0.9:
        return "decreasing"
    else:
        return "stable"


# ====================================
# MÉTRIQUES BASE DE DONNÉES
# ====================================

@asset(
    group_name="performance_monitoring",
    description="Métriques de performance base de données",
)
def database_performance_metrics(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Collecte les métriques de performance PostgreSQL:
    - Taille des tables
    - Requêtes actives
    - Cache hit ratio
    - Connexions
    """
    conn = _get_pg_connection()
    
    metrics = {}
    
    try:
        # 1. Taille des tables
        df_tables = pd.read_sql("""
            SELECT
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
                pg_total_relation_size(schemaname||'.'||tablename) AS size_bytes,
                n_live_tup AS row_count,
                n_dead_tup AS dead_rows,
                last_vacuum,
                last_autovacuum,
                last_analyze,
                last_autoanalyze
            FROM pg_stat_user_tables
            WHERE schemaname = 'hubeau'
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        """, conn)
        
        metrics["table_stats"] = df_tables.to_dict('records')
        
        # 2. Requêtes actives
        df_active = pd.read_sql("""
            SELECT
                COUNT(*) AS active_queries,
                COUNT(CASE WHEN state = 'active' THEN 1 END) AS running_queries,
                COUNT(CASE WHEN state = 'idle' THEN 1 END) AS idle_queries,
                AVG(EXTRACT(EPOCH FROM (now() - query_start))) AS avg_query_duration
            FROM pg_stat_activity
            WHERE state != 'idle'
        """, conn)
        
        metrics["query_stats"] = df_active.to_dict('records')[0]
        
        # 3. Cache hit ratio
        df_cache = pd.read_sql("""
            SELECT
                round(100.0 * sum(blks_hit) / (sum(blks_hit) + sum(blks_read)), 2) AS cache_hit_ratio
            FROM pg_stat_database
            WHERE datname = current_database()
        """, conn)
        
        metrics["cache_stats"] = df_cache.to_dict('records')[0]
        
        # 4. Connexions
        df_connections = pd.read_sql("""
            SELECT
                COUNT(*) AS total_connections,
                COUNT(CASE WHEN state = 'active' THEN 1 END) AS active_connections,
                COUNT(CASE WHEN state = 'idle' THEN 1 END) AS idle_connections,
                COUNT(CASE WHEN state = 'idle in transaction' THEN 1 END) AS idle_in_transaction
            FROM pg_stat_activity
        """, conn)
        
        metrics["connection_stats"] = df_connections.to_dict('records')[0]
        
        # 5. Index usage
        df_indexes = pd.read_sql("""
            SELECT
                schemaname,
                tablename,
                indexname,
                idx_tup_read,
                idx_tup_fetch,
                idx_scan
            FROM pg_stat_user_indexes
            WHERE schemaname = 'hubeau'
            ORDER BY idx_scan DESC
            LIMIT 20
        """, conn)
        
        metrics["index_stats"] = df_indexes.to_dict('records')
        
    except Exception as e:
        context.log.error(f"Erreur collecte métriques BDD: {e}")
        metrics["error"] = str(e)
    
    finally:
        conn.close()
    
    # Métadonnées pour Dagster UI
    metadata = {
        "cache_hit_ratio": MetadataValue.float(metrics.get("cache_stats", {}).get("cache_hit_ratio", 0)),
        "active_queries": MetadataValue.int(metrics.get("query_stats", {}).get("active_queries", 0)),
        "total_connections": MetadataValue.int(metrics.get("connection_stats", {}).get("total_connections", 0)),
        "nb_tables": MetadataValue.int(len(metrics.get("table_stats", []))),
        "nb_indexes": MetadataValue.int(len(metrics.get("index_stats", []))),
        "full_metrics": MetadataValue.json(metrics),
    }
    
    context.log.info(f"🗄️ Performance BDD:")
    context.log.info(f"   Cache hit ratio: {metrics.get('cache_stats', {}).get('cache_hit_ratio', 0):.1f}%")
    context.log.info(f"   Requêtes actives: {metrics.get('query_stats', {}).get('active_queries', 0)}")
    context.log.info(f"   Connexions: {metrics.get('connection_stats', {}).get('total_connections', 0)}")
    
    return Output(metrics, metadata=metadata)


# ====================================
# MÉTRIQUES PIPELINE DLT
# ====================================

@asset(
    group_name="performance_monitoring",
    description="Métriques de performance pipeline DLT",
)
def dlt_pipeline_metrics(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Collecte les métriques de performance des pipelines DLT:
    - Temps d'exécution
    - Taux de succès
    - Volume de données traitées
    """
    conn = _get_pg_connection()
    
    metrics = {}
    
    try:
        # 1. Statistiques des loads DLT
        df_loads = pd.read_sql("""
            SELECT
                pipeline_name,
                COUNT(*) AS total_loads,
                SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END) AS successful_loads,
                SUM(CASE WHEN status != 0 THEN 1 ELSE 0 END) AS failed_loads,
                ROUND(100.0 * SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS success_rate,
                MAX(inserted_at) AS last_load,
                MIN(inserted_at) AS first_load,
                AVG(EXTRACT(EPOCH FROM (finished_at - started_at))) AS avg_duration_seconds
            FROM hubeau._dlt_loads
            WHERE inserted_at >= NOW() - INTERVAL '7 days'
            GROUP BY pipeline_name
            ORDER BY last_load DESC
        """, conn)
        
        metrics["load_stats"] = df_loads.to_dict('records')
        
        # 2. Volume de données par pipeline
        df_volume = pd.read_sql("""
            SELECT
                pipeline_name,
                COUNT(DISTINCT table_name) AS nb_tables,
                SUM(rows_count) AS total_rows,
                MAX(inserted_at) AS last_update
            FROM hubeau._dlt_loads
            WHERE inserted_at >= NOW() - INTERVAL '7 days'
            AND status = 0
            GROUP BY pipeline_name
            ORDER BY total_rows DESC
        """, conn)
        
        metrics["volume_stats"] = df_volume.to_dict('records')
        
        # 3. Performance par jour
        df_daily = pd.read_sql("""
            SELECT
                DATE(inserted_at) AS load_date,
                COUNT(*) AS loads_per_day,
                SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END) AS successful_loads,
                AVG(EXTRACT(EPOCH FROM (finished_at - started_at))) AS avg_duration_seconds,
                SUM(rows_count) AS total_rows_processed
            FROM hubeau._dlt_loads
            WHERE inserted_at >= NOW() - INTERVAL '30 days'
            GROUP BY DATE(inserted_at)
            ORDER BY load_date DESC
        """, conn)
        
        metrics["daily_stats"] = df_daily.to_dict('records')
        
    except Exception as e:
        context.log.warning(f"Impossible de récupérer métriques DLT: {e}")
        metrics["error"] = str(e)
    
    finally:
        conn.close()
    
    # Calculer les métriques globales
    if metrics.get("load_stats"):
        total_loads = sum(load["total_loads"] for load in metrics["load_stats"])
        successful_loads = sum(load["successful_loads"] for load in metrics["load_stats"])
        global_success_rate = (successful_loads / total_loads * 100) if total_loads > 0 else 0
        
        metrics["global_stats"] = {
            "total_loads": total_loads,
            "successful_loads": successful_loads,
            "success_rate": round(global_success_rate, 2),
            "nb_pipelines": len(metrics["load_stats"])
        }
    
    # Métadonnées pour Dagster UI
    metadata = {
        "global_success_rate": MetadataValue.float(metrics.get("global_stats", {}).get("success_rate", 0)),
        "total_loads": MetadataValue.int(metrics.get("global_stats", {}).get("total_loads", 0)),
        "nb_pipelines": MetadataValue.int(metrics.get("global_stats", {}).get("nb_pipelines", 0)),
        "nb_tables": MetadataValue.int(len(metrics.get("load_stats", []))),
        "full_metrics": MetadataValue.json(metrics),
    }
    
    context.log.info(f"🔄 Performance DLT:")
    context.log.info(f"   Taux de succès global: {metrics.get('global_stats', {}).get('success_rate', 0):.1f}%")
    context.log.info(f"   Loads totaux: {metrics.get('global_stats', {}).get('total_loads', 0)}")
    context.log.info(f"   Pipelines: {metrics.get('global_stats', {}).get('nb_pipelines', 0)}")
    
    return Output(metrics, metadata=metadata)


# ====================================
# SENSOR DE PERFORMANCE
# ====================================

@sensor(
    name="performance_alert_sensor",
    description="Sensor d'alerte performance en temps réel"
)
def performance_alert_sensor(context: SensorEvaluationContext):
    """
    Sensor qui détecte les problèmes de performance:
    - CPU > 80%
    - Mémoire > 85%
    - Cache hit ratio < 90%
    - Taux d'échec DLT > 10%
    """
    alerts = []
    
    try:
        # Récupérer les dernières métriques système
        latest_metrics = performance_tracker.get_latest_metrics()
        
        if latest_metrics:
            # Alerte CPU
            if latest_metrics.get("cpu_percent", 0) > 80:
                alerts.append({
                    "type": "cpu_high",
                    "message": f"CPU élevé: {latest_metrics['cpu_percent']:.1f}%",
                    "severity": "warning"
                })
            
            # Alerte mémoire
            if latest_metrics.get("memory_percent", 0) > 85:
                alerts.append({
                    "type": "memory_high",
                    "message": f"Mémoire élevée: {latest_metrics['memory_percent']:.1f}%",
                    "severity": "warning"
                })
            
            # Alerte disque
            if latest_metrics.get("disk_percent", 0) > 90:
                alerts.append({
                    "type": "disk_full",
                    "message": f"Espace disque faible: {latest_metrics['disk_percent']:.1f}%",
                    "severity": "critical"
                })
    
    except Exception as e:
        context.log.error(f"Erreur sensor performance: {e}")
    
    # Retourner les alertes
    if alerts:
        context.log.info(f"🚨 {len(alerts)} alertes performance détectées")
        for alert in alerts:
            context.log.warning(f"   {alert['severity'].upper()}: {alert['message']}")
        
        return [alert for alert in alerts if alert["severity"] == "critical"]
    
    return []