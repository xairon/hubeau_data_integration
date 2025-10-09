"""
Extracteur parallèle optimisé pour Hub'Eau avec DLT
"""
import asyncio
import concurrent.futures
from typing import Iterator, Dict, Any, List, Optional
from threading import Semaphore
import time
from dataclasses import dataclass
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential


@dataclass
class ExtractionMetrics:
    """Métriques d'extraction pour monitoring"""
    total_stations: int = 0
    processed_stations: int = 0
    total_records: int = 0
    api_calls: int = 0
    start_time: float = 0
    
    @property
    def elapsed_time(self) -> float:
        return time.time() - self.start_time
    
    @property
    def stations_per_second(self) -> float:
        if self.elapsed_time == 0:
            return 0
        return self.processed_stations / self.elapsed_time
    
    @property
    def progress_percent(self) -> float:
        if self.total_stations == 0:
            return 0
        return (self.processed_stations / self.total_stations) * 100


class ParallelHubeauExtractor:
    """Extracteur parallèle pour les APIs Hub'Eau"""
    
    def __init__(
        self,
        config: Dict[str, Any],
        max_workers: int = 10,
        max_concurrent_api_calls: int = 5,
        batch_size: int = 10000,
        dagster_log=None
    ):
        self.config = config
        self.max_workers = max_workers
        self.api_semaphore = Semaphore(max_concurrent_api_calls)
        self.batch_size = batch_size
        self.dagster_log = dagster_log
        self.metrics = ExtractionMetrics(start_time=time.time())
        
    def log(self, message: str):
        """Log avec Dagster si disponible"""
        if self.dagster_log:
            self.dagster_log.info(message)
        else:
            print(f"[ParallelExtractor] {message}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def extract_station_data(
        self,
        station: Dict[str, Any],
        client: httpx.Client,
        params: Dict[str, Any]
    ) -> Iterator[Dict[str, Any]]:
        """Extrait les données pour une station avec retry"""
        with self.api_semaphore:
            self.metrics.api_calls += 1
            
            # Paramètres spécifiques à la station
            station_params = params.copy()
            station_code_field = self.config.get("station_code_field", "code_station")
            station_params[station_code_field] = station[station_code_field]
            
            # Pagination
            page = 1
            total_records = 0
            
            while True:
                station_params["page"] = page
                
                try:
                    response = client.get(
                        self.config["api_endpoint"],
                        params=station_params,
                        timeout=60.0
                    )
                    response.raise_for_status()
                    
                    data = response.json()
                    records = data.get("data", [])
                    
                    if not records:
                        break
                    
                    # Enrichir avec info station
                    for record in records:
                        record["_station_code"] = station[station_code_field]
                        record["_station_libelle"] = station.get("libelle_station", "")
                        yield record
                    
                    total_records += len(records)
                    
                    # Vérifier s'il y a d'autres pages
                    if len(records) < station_params.get("size", 5000):
                        break
                        
                    page += 1
                    
                except Exception as e:
                    self.log(f"⚠️ Erreur station {station[station_code_field]}: {str(e)}")
                    raise
            
            self.metrics.total_records += total_records
            self.metrics.processed_stations += 1
            
            # Log progression
            if self.metrics.processed_stations % 10 == 0:
                self.log(
                    f"📊 Progression: {self.metrics.progress_percent:.1f}% "
                    f"({self.metrics.processed_stations}/{self.metrics.total_stations}) "
                    f"- {self.metrics.stations_per_second:.2f} stations/s"
                )
    
    def extract_parallel(
        self,
        stations: List[Dict[str, Any]],
        client: httpx.Client,
        params: Dict[str, Any]
    ) -> Iterator[Dict[str, Any]]:
        """Extraction parallèle de toutes les stations"""
        self.metrics.total_stations = len(stations)
        self.log(f"🚀 Démarrage extraction parallèle pour {len(stations)} stations...")
        
        # Utiliser ThreadPoolExecutor pour l'I/O
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Lancer toutes les extractions
            future_to_station = {
                executor.submit(
                    lambda s: list(self.extract_station_data(s, client, params)),
                    station
                ): station
                for station in stations
            }
            
            # Collecter les résultats au fur et à mesure
            batch = []
            for future in concurrent.futures.as_completed(future_to_station):
                try:
                    station_records = future.result()
                    
                    # Yield par batch pour optimiser la mémoire
                    for record in station_records:
                        batch.append(record)
                        if len(batch) >= self.batch_size:
                            yield from batch
                            batch = []
                            
                except Exception as e:
                    station = future_to_station[future]
                    self.log(f"❌ Erreur pour station {station}: {str(e)}")
            
            # Yield les derniers enregistrements
            if batch:
                yield from batch
        
        # Rapport final
        self.log(
            f"✅ Extraction terminée: {self.metrics.total_records} enregistrements "
            f"en {self.metrics.elapsed_time:.2f}s "
            f"({self.metrics.total_records/self.metrics.elapsed_time:.0f} records/s)"
        )


def create_parallel_source(
    config: Dict[str, Any],
    stations: List[Dict[str, Any]],
    client: httpx.Client,
    dagster_log=None,
    use_parallel: bool = True
) -> Iterator[Dict[str, Any]]:
    """
    Factory pour créer une source parallèle ou séquentielle selon la config
    """
    # Déterminer si on utilise l'extraction parallèle
    if not use_parallel or len(stations) < 10:
        # Extraction séquentielle pour peu de stations
        dagster_log.info(f"📝 Extraction séquentielle pour {len(stations)} stations")
        for station in stations:
            # Code d'extraction séquentielle existant
            yield from extract_sequential(station, config, client)
    else:
        # Extraction parallèle pour beaucoup de stations
        extractor = ParallelHubeauExtractor(
            config=config,
            max_workers=min(10, len(stations) // 5),  # Adapter selon le nombre
            max_concurrent_api_calls=5,  # Respecter les limites API
            batch_size=10000,
            dagster_log=dagster_log
        )
        
        # Créer les paramètres de base
        params = {
            "size": config.get("pagination", {}).get("size", 5000),
            "format": "json"
        }
        
        # Ajouter les filtres temporels si définis
        if "temporal_filter" in config:
            params.update(config["temporal_filter"])
        
        yield from extractor.extract_parallel(stations, client, params)


def extract_sequential(
    station: Dict[str, Any],
    config: Dict[str, Any],
    client: httpx.Client
) -> Iterator[Dict[str, Any]]:
    """Extraction séquentielle classique (fallback)"""
    # Implémentation existante
    pass
