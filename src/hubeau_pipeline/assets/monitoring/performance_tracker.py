"""
Tracking et monitoring des performances des pipelines DLT
"""
import time
import json
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any
from pathlib import Path
from datetime import datetime
import statistics


@dataclass
class PipelineMetrics:
    """Métriques détaillées pour un run de pipeline"""
    pipeline_name: str
    source_name: str
    start_time: float
    end_time: float = 0
    
    # Métriques d'extraction
    extraction_duration: float = 0
    api_calls: int = 0
    records_extracted: int = 0
    stations_processed: int = 0
    
    # Métriques d'écriture
    write_duration: float = 0
    records_written: int = 0
    files_created: int = 0
    bytes_written: int = 0
    
    # Métriques de performance
    used_parallel_extraction: bool = False
    workers_count: int = 1
    page_size: int = 1000
    rate_limit_rps: float = 1.0
    
    # Erreurs
    errors_count: int = 0
    retries_count: int = 0
    
    @property
    def total_duration(self) -> float:
        return self.end_time - self.start_time if self.end_time else 0
    
    @property
    def extraction_throughput(self) -> float:
        """Records par seconde pendant l'extraction"""
        if self.extraction_duration == 0:
            return 0
        return self.records_extracted / self.extraction_duration
    
    @property
    def write_throughput(self) -> float:
        """Records par seconde pendant l'écriture"""
        if self.write_duration == 0:
            return 0
        return self.records_written / self.write_duration
    
    @property
    def api_calls_per_second(self) -> float:
        """Appels API par seconde"""
        if self.extraction_duration == 0:
            return 0
        return self.api_calls / self.extraction_duration
    
    @property
    def mb_per_second(self) -> float:
        """MB écrits par seconde"""
        if self.write_duration == 0:
            return 0
        return (self.bytes_written / 1024 / 1024) / self.write_duration
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir en dict pour JSON"""
        data = asdict(self)
        # Ajouter les propriétés calculées
        data.update({
            "total_duration": self.total_duration,
            "extraction_throughput": self.extraction_throughput,
            "write_throughput": self.write_throughput,
            "api_calls_per_second": self.api_calls_per_second,
            "mb_per_second": self.mb_per_second,
            "timestamp": datetime.fromtimestamp(self.start_time).isoformat()
        })
        return data


class PerformanceTracker:
    """Tracker global des performances"""
    
    def __init__(self, metrics_dir: Path = Path("metrics")):
        self.metrics_dir = metrics_dir
        self.metrics_dir.mkdir(exist_ok=True)
        self.current_metrics: Dict[str, PipelineMetrics] = {}
        self.historical_metrics: List[PipelineMetrics] = []
        self._load_history()
    
    def start_pipeline(
        self,
        pipeline_name: str,
        source_name: str,
        **kwargs
    ) -> PipelineMetrics:
        """Démarrer le tracking d'un pipeline"""
        metrics = PipelineMetrics(
            pipeline_name=pipeline_name,
            source_name=source_name,
            start_time=time.time(),
            **kwargs
        )
        self.current_metrics[source_name] = metrics
        return metrics
    
    def update_metrics(self, source_name: str, **updates):
        """Mettre à jour les métriques en cours"""
        if source_name in self.current_metrics:
            metrics = self.current_metrics[source_name]
            for key, value in updates.items():
                if hasattr(metrics, key):
                    setattr(metrics, key, value)
    
    def end_pipeline(self, source_name: str) -> Optional[PipelineMetrics]:
        """Terminer le tracking d'un pipeline"""
        if source_name not in self.current_metrics:
            return None
            
        metrics = self.current_metrics[source_name]
        metrics.end_time = time.time()
        
        # Sauvegarder dans l'historique
        self.historical_metrics.append(metrics)
        self._save_metrics(metrics)
        
        # Retirer des métriques courantes
        del self.current_metrics[source_name]
        
        return metrics
    
    def _save_metrics(self, metrics: PipelineMetrics):
        """Sauvegarder les métriques dans un fichier"""
        # Fichier du jour
        date_str = datetime.now().strftime("%Y-%m-%d")
        metrics_file = self.metrics_dir / f"performance_{date_str}.jsonl"
        
        # Ajouter au fichier JSONL
        with open(metrics_file, "a") as f:
            json.dump(metrics.to_dict(), f)
            f.write("\n")
    
    def _load_history(self):
        """Charger l'historique des métriques"""
        for metrics_file in self.metrics_dir.glob("performance_*.jsonl"):
            with open(metrics_file) as f:
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        # Reconstruire PipelineMetrics depuis le dict
                        # (simplification, en pratique il faudrait un from_dict)
                        pass
    
    def get_statistics(self, source_name: str, days: int = 7) -> Dict[str, Any]:
        """Obtenir des statistiques sur les derniers jours"""
        # Filtrer les métriques récentes pour cette source
        recent_metrics = [
            m for m in self.historical_metrics
            if m.source_name == source_name
            and (time.time() - m.start_time) < (days * 86400)
        ]
        
        if not recent_metrics:
            return {}
        
        # Calculer les statistiques
        return {
            "runs_count": len(recent_metrics),
            "avg_duration": statistics.mean([m.total_duration for m in recent_metrics]),
            "avg_records": statistics.mean([m.records_extracted for m in recent_metrics]),
            "avg_throughput": statistics.mean([m.extraction_throughput for m in recent_metrics]),
            "max_throughput": max([m.extraction_throughput for m in recent_metrics]),
            "total_records": sum([m.records_extracted for m in recent_metrics]),
            "error_rate": sum([m.errors_count for m in recent_metrics]) / len(recent_metrics)
        }
    
    def generate_report(self, output_file: Path = Path("performance_report.md")):
        """Générer un rapport de performance"""
        with open(output_file, "w") as f:
            f.write("# 📊 Rapport de Performance Hub'Eau\n\n")
            f.write(f"Généré le: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Statistiques par source
            f.write("## Performance par Source\n\n")
            f.write("| Source | Runs | Durée Moy. | Records/s | Erreurs |\n")
            f.write("|--------|------|------------|-----------|----------|\n")
            
            sources = set(m.source_name for m in self.historical_metrics)
            for source in sorted(sources):
                stats = self.get_statistics(source)
                if stats:
                    f.write(
                        f"| {source} | {stats['runs_count']} | "
                        f"{stats['avg_duration']:.1f}s | "
                        f"{stats['avg_throughput']:.0f} | "
                        f"{stats['error_rate']:.1%} |\n"
                    )
            
            # Top performers
            f.write("\n## 🏆 Top Performers\n\n")
            sorted_metrics = sorted(
                self.historical_metrics,
                key=lambda m: m.extraction_throughput,
                reverse=True
            )[:10]
            
            for i, m in enumerate(sorted_metrics, 1):
                f.write(
                    f"{i}. **{m.source_name}**: "
                    f"{m.extraction_throughput:.0f} records/s "
                    f"({m.records_extracted} records en {m.extraction_duration:.1f}s)\n"
                )


# Instance globale
performance_tracker = PerformanceTracker()
