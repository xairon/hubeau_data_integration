"""
Station API extraction logic for Hub'Eau data ingestion.
Extracts active station codes with their data months from Hub'Eau APIs.
"""
import calendar
import logging
import traceback
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import httpx


def extract_station_codes_from_result(
    result: Dict[str, Any],
    station_type: str = "temperature",
    partition_date: str = None,
    logger: Optional[logging.Logger] = None
) -> Dict[str, List[str]]:
    """
    Extrait les codes de stations actives avec leurs mois de données depuis l'API Hub'Eau.

    Args:
        result: Résultat de l'asset upstream (peut être vide {})
        station_type: Type de stations ("temperature", "hydrometry", "piezometry", etc.)
        partition_date: Date de partition pour filtrer les stations actives (REQUIS)
        logger: Optional logger instance. If not provided, uses the module logger.

    Returns:
        Dict[str, List[str]]: Dictionnaire {code_station: [liste des mois "YYYY-MM" avec données]}
    """
    # Use provided logger or create a module-level one
    if logger is None:
        logger = logging.getLogger(__name__)

    if not partition_date:
        logger.warning("⚠️ Pas de partition_date fournie, récupération depuis PostgreSQL (liste potentiellement incomplète)")
        # Import here to avoid circular dependencies
        from src.hubeau_pipeline.utils.station_postgres import extract_station_codes_from_postgres

        # Convertir la liste en dict avec tous les mois de l'année courante
        stations_list = extract_station_codes_from_postgres(station_type)
        year = datetime.now().year
        all_months = [f"{year}-{m:02d}" for m in range(1, 13)]
        return {station: all_months for station in stations_list}

    # ✅ SOLUTION: Récupérer directement les stations actives depuis l'API Hub'Eau
    logger.info(f"🔍 Récupération des stations actives depuis l'API Hub'Eau pour {station_type} (partition: {partition_date})")

    try:
        # ✅ CORRECTION: Utiliser la même logique que DLT pour la période
        # DLT utilise month_windows() qui génère des fenêtres mensuelles exactes
        year = datetime.strptime(partition_date, "%Y-%m-%d").year

        # ✅ NOUVELLE APPROCHE: Tester chaque mois comme le fait DLT
        # Au lieu d'une seule requête sur toute l'année, faire une requête par mois
        logger.info(f"🔍 Test des stations actives pour l'année {year}")

        # Configuration des endpoints par type
        # ✅ STRATÉGIE: Utiliser l'endpoint de STATIONS si filtrage temporel supporté
        # ❌ SINON: Utiliser l'endpoint de DONNÉES avec approche hybride (mois + département si limite 20K)
        api_configs = {
            "temperature": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/temperature",
                "path": "/station",  # ✅ Endpoint stations supporte filtrage temporel
                "start_param": "date_debut_mesure",
                "end_param": "date_fin_mesure",
                "station_field": "code_station"
            },
            "hydrometry": {
                "base_url": "https://hubeau.eaufrance.fr/api/v2/hydrometrie",
                "path": "/obs_elab",  # ✅ Observations élaborées (historique complet)
                "start_param": "date_debut_obs_elab",
                "end_param": "date_fin_obs_elab",
                "station_field": "code_station"
            },
            "piezometry": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/niveaux_nappes",
                "path": "/stations",  # ✅ CORRECTION: Utiliser /stations avec date_recherche (ne nécessite pas code_bss)
                "start_param": "date_recherche",  # ✅ /stations utilise date_recherche (une seule date)
                "end_param": None,  # ✅ /stations n'a pas de paramètre de fin
                "station_field": "code_bss"
            },
            "quality_rivers": {
                "base_url": "https://hubeau.eaufrance.fr/api/v2/qualite_rivieres",
                "path": "/station_pc",  # ✅ Endpoint stations supporte filtrage temporel (4431 vs 24324)
                "start_param": "date_debut_prelevement",
                "end_param": "date_fin_prelevement",
                "station_field": "code_station"
            },
            "quality_groundwater": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/qualite_nappes",
                "path": "/analyses",  # ❌ /stations ne supporte pas filtrage, utiliser données
                "start_param": "date_debut_prelevement",
                "end_param": "date_fin_prelevement",
                "station_field": "code_bss"
            },
            "ecoulement": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/ecoulement",
                "path": "/observations",  # ❌ /stations ne supporte pas filtrage, utiliser données
                "start_param": "date_observation_min",  # ✅ Corrigé selon API
                "end_param": "date_observation_max",    # ✅ Corrigé selon API
                "station_field": "code_station"
            },
            "hydrobio": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/hydrobio",
                "path": "/indices",  # ❌ Pas d'endpoint stations avec filtrage, utiliser données
                "start_param": "date_debut_prelevement",  # ✅ Corrigé selon API
                "end_param": "date_fin_prelevement",      # ✅ Corrigé selon API
                "station_field": "code_station_hydrobio"
            },
            "prelevements": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/prelevements",
                "path": "/chroniques",  # ❌ Pas d'endpoint stations avec filtrage, utiliser données
                "start_param": "annee",  # ⚠️ Chroniques: filtrage par année uniquement (pas de dates précises)
                "end_param": "annee",    # ⚠️ Utiliser la même année pour start/end
                "station_field": "code_ouvrage"
            }
        }

        if station_type not in api_configs:
            logger.warning(f"⚠️ Type de station non supporté: {station_type}, fallback sur PostgreSQL")
            from src.hubeau_pipeline.utils.station_postgres import extract_station_codes_from_postgres
            stations_list = extract_station_codes_from_postgres(station_type)
            # Convert list to dict with all months of current year
            year = datetime.now().year
            all_months = [f"{year}-{m:02d}" for m in range(1, 13)]
            return {station: all_months for station in stations_list}

        config = api_configs[station_type]

        # ✅ NOUVELLE APPROCHE: Tester chaque mois comme le fait DLT
        # Utiliser month_windows() pour générer les mêmes fenêtres que DLT
        def month_windows(start: date, end: date):
            """Génère les mêmes fenêtres mensuelles que DLT"""
            cursor = date(start.year, start.month, 1)
            final = date(end.year, end.month, calendar.monthrange(end.year, end.month)[1])
            while cursor <= final:
                last_day = calendar.monthrange(cursor.year, cursor.month)[1]
                period_end = date(cursor.year, cursor.month, last_day)
                yield cursor, min(period_end, final)
                if cursor.month == 12:
                    cursor = date(cursor.year + 1, 1, 1)
                else:
                    cursor = date(cursor.year, cursor.month + 1, 1)

        # Générer les fenêtres mensuelles pour l'année
        start_date_obj = date(year, 1, 1)
        end_date_obj = date(year, 12, 31)

        # ✅ NOUVELLE STRUCTURE: Dict[station_code, List[month_str]]
        stations_months: Dict[str, set] = {}  # Utiliser set temporairement pour éviter doublons
        total_requests = 0

        # ✅ NOUVELLE APPROCHE: Toujours utiliser découpage mensuel pour tracker les mois actifs
        logger.info(f"🔍 Interrogation API par mois pour {station_type} ({year})...")

        station_field = config["station_field"]

        # Découpage mensuel pour tous les types : permet de tracker les mois actifs par station
        for month_start, month_end in month_windows(start_date_obj, end_date_obj):
            month_str = month_start.strftime('%Y-%m')
            page = 1
            month_stations = set()

            # ✅ PAGINATION: Continue jusqu'à ce qu'on ait toutes les stations du mois
            while True:
                params = {
                    "format": "json",
                    "size": 20000,
                    "page": page,
                }

                # ✅ GESTION SPÉCIALE pour piezometry: date_recherche prend UNE seule date (milieu du mois)
                if station_type == "piezometry":
                    # Utiliser milieu du mois pour maximiser couverture
                    mid_month = month_start.replace(day=15)
                    params[config["start_param"]] = mid_month.isoformat()
                else:
                    # Pour les autres types, utiliser start/end normalement
                    params[config["start_param"]] = month_start.isoformat()
                    if config["end_param"] is not None:
                        params[config["end_param"]] = month_end.isoformat()

                try:
                    response = httpx.get(f"{config['base_url']}{config['path']}", params=params, timeout=60)
                    total_requests += 1

                    if response.status_code not in [200, 206]:
                        break

                    data = response.json()
                    records = data.get("data", [])

                    if len(records) == 0:
                        break

                    # Extraire les stations du mois
                    for record in records:
                        if station_field in record:
                            station_code = record[station_field]
                            month_stations.add(station_code)

                    # Si moins de 20K records, c'est la dernière page
                    if len(records) < 20000:
                        break

                    page += 1

                except Exception as e:
                    break

            # Ajouter ce mois à chaque station trouvée
            for station_code in month_stations:
                if station_code not in stations_months:
                    stations_months[station_code] = set()
                stations_months[station_code].add(month_str)

            if len(month_stations) > 0:
                logger.info(f"  Mois {month_str}: {len(month_stations)} stations ({page} page(s))")

        # Convertir les sets en listes triées
        stations_months_dict = {
            station: sorted(list(months))
            for station, months in stations_months.items()
        }

        total_stations = len(stations_months_dict)
        total_station_months = sum(len(months) for months in stations_months_dict.values())
        logger.info(f"✅ {total_stations} stations actives trouvées pour {station_type} (année {year})")
        logger.info(f"📊 Total station-mois: {total_station_months} ({total_requests} requêtes)")

        return stations_months_dict

    except Exception as e:
        logger.error(f"⚠️ Erreur lors de la récupération des stations depuis l'API: {e}")
        logger.error(traceback.format_exc())
        logger.warning("⚠️ Fallback sur la liste PostgreSQL (potentiellement incomplète)")

        # Import here to avoid circular dependencies
        from src.hubeau_pipeline.utils.station_postgres import extract_station_codes_from_postgres

        # Convertir la liste en dict avec tous les mois de l'année
        stations_list = extract_station_codes_from_postgres(station_type)
        year = datetime.strptime(partition_date, "%Y-%m-%d").year if partition_date else datetime.now().year
        all_months = [f"{year}-{m:02d}" for m in range(1, 13)]
        return {station: all_months for station in stations_list}


def get_active_departments_for_stations(
    stations_data: Dict[str, List[str]],
    station_type: str,
    logger: Optional[logging.Logger] = None
) -> List[str]:
    """
    Extrait les départements ayant des stations actives.

    Args:
        stations_data: Dict des stations actives {code_station: [mois]}
        station_type: Type de stations
        logger: Optional logger instance. If not provided, uses the module logger.

    Returns:
        Liste unique et triée des départements ayant des stations
    """
    # Use provided logger or create a module-level one
    if logger is None:
        logger = logging.getLogger(__name__)

    if not stations_data:
        return []

    # Convertir le dict en liste de codes stations
    station_codes = list(stations_data.keys())

    # Configuration des endpoints pour récupérer les métadonnées des stations
    api_configs = {
        "temperature": {
            "url": "https://hubeau.eaufrance.fr/api/v1/temperature/station",
            "station_field": "code_station",
            "dept_field": "code_departement"
        },
        "hydrometry": {
            "url": "https://hubeau.eaufrance.fr/api/v2/hydrometrie/referentiel/stations",
            "station_field": "code_station",
            "dept_field": "code_departement"
        },
        "piezometry": {
            "url": "https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/stations",
            "station_field": "code_bss",
            "dept_field": "code_departement"
        },
        "quality_rivers": {
            "url": "https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/station_pc",
            "station_field": "code_station",
            "dept_field": "code_departement"
        },
        "quality_groundwater": {
            "url": "https://hubeau.eaufrance.fr/api/v1/qualite_nappes/stations",
            "station_field": "code_bss",
            "dept_field": "code_departement"
        },
        "hydrobio": {
            "url": "https://hubeau.eaufrance.fr/api/v1/hydrobio/stations_hydrobio",
            "station_field": "code_station_hydrobio",
            "dept_field": "code_departement"
        },
        "ecoulement": {
            "url": "https://hubeau.eaufrance.fr/api/v1/ecoulement/stations",
            "station_field": "code_station",
            "dept_field": "code_departement"
        },
        "prelevements": {
            "url": "https://hubeau.eaufrance.fr/api/v1/prelevements/referentiel/ouvrages",  # ✅ CORRECTION: Utiliser ouvrages (chroniques utilisent code_ouvrage)
            "station_field": "code_ouvrage",  # ✅ CORRECTION: code_ouvrage pour les chroniques
            "dept_field": "code_departement"
        }
    }

    if station_type not in api_configs:
        logger.warning(f"⚠️ Type de station non supporté pour filtrage départements: {station_type}")
        return []

    config = api_configs[station_type]
    departments = set()

    try:
        # Stratégie: Récupérer en une seule fois toutes les stations
        # (plus efficace que requête par station)
        logger.info(f"🔍 Récupération départements pour {len(station_codes)} stations {station_type}")

        # Construire requête avec toutes les stations (si API supporte multi-stations)
        # Sinon, faire par batches
        station_field = config["station_field"]
        dept_field = config["dept_field"]

        # Stratégie batch : récupérer toutes les stations en plusieurs requêtes
        batch_size = 100  # Limiter par sécurité
        for i in range(0, len(station_codes), batch_size):
            batch = station_codes[i:i + batch_size]

            # Requête pour ce batch
            params = {
                "format": "json",
                "size": 1000,
                station_field: ",".join(batch)
            }

            try:
                response = httpx.get(config["url"], params=params, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    for record in data.get("data", []):
                        dept = record.get(dept_field)
                        if dept:
                            departments.add(dept)
            except Exception as e:
                logger.warning(f"⚠️ Erreur lors de la requête batch {i//batch_size + 1}: {e}")
                continue

        departments_list = sorted(list(departments))
        logger.info(f"✅ {len(departments_list)} départements avec stations actives trouvés")
        return departments_list

    except Exception as e:
        logger.error(f"⚠️ Erreur lors du filtrage départements: {e}")
        logger.error(traceback.format_exc())
        return []