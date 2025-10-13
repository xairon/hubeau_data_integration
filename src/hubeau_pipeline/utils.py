"""
Utilitaires partagés pour la robustesse et les retry
"""

import time
import random
import httpx
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

def get_with_backoff(
    client: httpx.Client, 
    url: str, 
    params: Dict[str, Any], 
    max_tries: int = 5,
    base_delay: float = 1.0
) -> httpx.Response:
    """
    Requête HTTP avec backoff exponentiel et retry automatique
    
    Args:
        client: Client httpx
        url: URL à appeler
        params: Paramètres de la requête
        max_tries: Nombre maximum de tentatives
        base_delay: Délai de base en secondes
    
    Returns:
        Response httpx
        
    Raises:
        httpx.HTTPError: Si toutes les tentatives échouent
    """
    last_exception = None
    
    for attempt in range(max_tries):
        try:
            response = client.get(url, params=params)
            
            # Succès
            if response.status_code == 200:
                return response
            
            # Erreurs temporaires -> retry
            if response.status_code in (429, 500, 502, 503, 504):
                if attempt < max_tries - 1:  # Pas la dernière tentative
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(delay)
                    continue
            
            # Autres erreurs -> pas de retry
            response.raise_for_status()
            return response
            
        except httpx.RequestError as e:
            last_exception = e
            if attempt < max_tries - 1:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                time.sleep(delay)
                continue
    
    # Toutes les tentatives ont échoué
    if last_exception:
        raise last_exception
    else:
        raise httpx.HTTPError(f"Failed after {max_tries} attempts")

def fetch_hubeau_with_retry(
    client: httpx.Client,
    base_url: str, 
    params: Dict[str, Any],
    max_tries: int = 5
) -> list:
    """
    Pagination Hub'Eau avec retry automatique
    
    Args:
        client: Client httpx
        base_url: URL de base Hub'Eau
        params: Paramètres de la requête
        max_tries: Nombre maximum de tentatives par page
    
    Returns:
        Liste de tous les enregistrements
    """
    all_rows = []
    page = 1
    
    while True:
        try:
            page_params = {**params, "page": page, "size": 10000}
            response = get_with_backoff(client, base_url, page_params, max_tries)
            
            data = response.json()
            rows = data.get("data", [])
            all_rows.extend(rows)
            
            total_pages = data.get("totalPages", 1)
            if page >= total_pages:
                break
                
            page += 1
            
            # Rate limiting respectueux
            time.sleep(0.1)
            
        except Exception as e:
            # Log l'erreur et arrête la pagination
            logger.error(f"Error fetching page {page} from {base_url}: {e}")
            break
    
    return all_rows

def validate_coordinates(lon: Optional[float], lat: Optional[float]) -> bool:
    """
    Validation des coordonnées géographiques
    
    Args:
        lon: Longitude
        lat: Latitude
    
    Returns:
        True si les coordonnées sont valides
    """
    if lon is None or lat is None:
        return False
    
    return -180 <= lon <= 180 and -90 <= lat <= 90

def normalize_station_code(raw_code: str) -> Optional[str]:
    """
    Normalisation des codes de station
    
    Args:
        raw_code: Code brut de la station
    
    Returns:
        Code normalisé ou None si invalide
    """
    if not raw_code or not isinstance(raw_code, str):
        return None
    
    # Suppression des espaces et conversion en majuscules
    normalized = raw_code.strip().upper()
    
    # Validation basique (au moins 3 caractères)
    if len(normalized) < 3:
        return None
    
    return normalized

def create_upsert_sql(table_name: str, columns: list, conflict_columns: list) -> str:
    """
    Génération d'une requête SQL d'upsert (INSERT ... ON CONFLICT)
    
    Args:
        table_name: Nom de la table
        columns: Liste des colonnes
        conflict_columns: Colonnes de conflit pour ON CONFLICT
    
    Returns:
        Requête SQL d'upsert
    """
    columns_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    
    conflict_str = ", ".join(conflict_columns)
    update_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_columns])
    
    return f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_str})
        DO UPDATE SET {update_str}
    """

def log_asset_metrics(context, asset_name: str, metrics: Dict[str, Any]):
    """
    Log des métriques d'asset avec formatage standardisé
    
    Args:
        context: Contexte Dagster
        asset_name: Nom de l'asset
        metrics: Dictionnaire des métriques
    """
    log = context.log
    
    log.info(f"=== {asset_name} - Métriques ===")
    for key, value in metrics.items():
        log.info(f"  {key}: {value}")
    log.info(f"=== Fin métriques {asset_name} ===")

class RateLimiter:
    """Rate limiter simple avec token bucket"""
    
    def __init__(self, max_requests: int = 10, time_window: int = 60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
    
    def can_make_request(self) -> bool:
        """Vérifie si une requête peut être faite"""
        now = time.time()
        
        # Nettoyage des anciennes requêtes
        self.requests = [req_time for req_time in self.requests if now - req_time < self.time_window]
        
        return len(self.requests) < self.max_requests
    
    def record_request(self):
        """Enregistre une requête"""
        self.requests.append(time.time())
    
    def wait_if_needed(self):
        """Attend si nécessaire pour respecter le rate limit"""
        if not self.can_make_request():
            # Attendre jusqu'à ce qu'une requête soit libérée
            oldest_request = min(self.requests)
            wait_time = self.time_window - (time.time() - oldest_request) + 0.1
            if wait_time > 0:
                time.sleep(wait_time)
