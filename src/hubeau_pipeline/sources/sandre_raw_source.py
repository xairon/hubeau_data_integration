"""
Source DLT pour SANDRE - Charge tous les référentiels bruts
"""

import dlt
import requests
import csv
import time
import logging
import itertools
import re

from io import StringIO
from typing import Iterator, Dict, Any, Iterable, List, Optional, Set

logger = logging.getLogger(__name__)

BASE_URL = "https://api.sandre.eaufrance.fr/referentiels/v1"

SANDRE_RESOURCE_CONFIG = {
    "sandre_parametres": {"endpoint": "parametre", "page_size": 2000},
    "sandre_unites": {"endpoint": "unite", "page_size": 1000},
    "sandre_methodes": {"endpoint": "methode", "page_size": 1000},
    "sandre_supports": {"endpoint": "support", "page_size": 100},
    "sandre_fractions": {"endpoint": "fractionanalysee", "page_size": 50},
    "sandre_communes": {"endpoint": "commune", "page_size": 5000},
    "sandre_departements": {"endpoint": "departement", "page_size": 200},
    "sandre_regions": {"endpoint": "region", "page_size": 50},
    "sandre_intervenants": {"endpoint": "intervenant", "page_size": 5000},
    "sandre_masses_eau_sout": {"endpoint": "massedeausouterraine", "page_size": 1000},
    "sandre_masses_eau_surf": {"endpoint": "massedeau", "page_size": 5000},
    "sandre_cours_eau": {"endpoint": "courseau", "page_size": 10000},
    "sandre_bassins": {"endpoint": "circonscriptionbassin", "page_size": 500},
    "sandre_milieux": {"endpoint": "milieu", "page_size": 50},
}

# Filtres de colonnes pour éviter la limite PostgreSQL de 1600 colonnes
# Pour la table parametres, ne garder que les colonnes essentielles
COLUMN_KEEP_PATTERNS = {
    "parametre": [
        r"^CdParametre",
        r"^NomParametre",
        r"^StParametre",
        r"^Date",
        r"^Auteur",
        r"^Lb.*Parametre$",  # Libellés courts/longs
        r"^DfParametre",  # Définition
        r"^Parametre(Calcule|Chimique|Hydrologique|Microbiologique|Physique|Environnemental)",
        r"^CdSubstanceChimique",
        r"^CdCASSubstanceChimique",
        r"^NomIUPAC",
        r"^FormuleBrute",
        r"^NomInternational",
        # Garder seulement 3 synonymes max
        r"^CdSynonymeParametre[1-3]$",
        r"^LbSynonymeParametre[1-3]$",  # Modifié de Synonyme à LbSynonymeParametre
        # Garder seulement 3 méthodes max
        r"^CdMethode[1-3]$",
        r"^NomMethode[1-3]$",
        # Garder seulement 2 unités max
        r"^CdUniteReference[1-2]$",
        r"^SymboleUniteReference[1-2]$",
        r"^LbUniteReference[1-2]$",  # Modifié de LibelleUnite à LbUniteReference
        # Garder seulement 2 fractions max
        r"^CdFractionAnalysee[1-2]$",
        r"^NatureFractionAnalysee[1-2]$",  # Modifié de FractionAnalysee à NatureFractionAnalysee
        # Support
        r"^CdSupport$",
        r"^NomSupport$",
        # Commentaires
        r"^Commentaire",
    ]
}


def filter_columns(columns: List[str], endpoint: str) -> Set[str]:
    """Filtre les colonnes selon les patterns définis pour éviter la limite PostgreSQL

    Args:
        columns: Liste des noms de colonnes
        endpoint: Nom de l'endpoint SANDRE

    Returns:
        Set des colonnes à garder
    """
    patterns = COLUMN_KEEP_PATTERNS.get(endpoint)
    if not patterns:
        # Pas de filtre défini, garder toutes les colonnes
        return set(columns)

    kept_columns = set()
    for col in columns:
        for pattern in patterns:
            if re.match(pattern, col):
                kept_columns.add(col)
                break

    logger.info(f"Column filtering for {endpoint}: {len(columns)} → {len(kept_columns)} columns")
    return kept_columns


def fetch_csv_data(endpoint: str, size: int = 10000) -> Iterator[Dict[str, Any]]:
    """Helper pour récupérer les données CSV depuis l'API SANDRE

    Note: L'API SANDRE retourne 2 lignes de header:
    - Ligne 1: Noms anglais des colonnes (codes)
    - Ligne 2: Descriptions françaises (à ignorer)
    - Ligne 3+: Données réelles
    """
    url = f"{BASE_URL}/{endpoint}.csv"
    page = 1
    columns_to_keep = None  # Will be set on first page

    while True:
        params = {"size": size, "page": page}

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            if not response.text or response.text.strip() == "":
                logger.info(f"{endpoint}: empty response, stopping pagination")
                break

            # Split lines and skip the 2nd header (descriptions françaises)
            lines = response.text.strip().split('\n')
            if len(lines) < 3:  # Need at least: header + description + 1 data row
                logger.info(f"{endpoint}: not enough lines ({len(lines)}), stopping")
                break

            # Reconstruct CSV without the 2nd line (French descriptions)
            csv_content = lines[0] + '\n' + '\n'.join(lines[2:])

            reader = csv.DictReader(StringIO(csv_content), delimiter=';')
            rows_loaded = 0

            # Apply column filtering on first page
            if page == 1 and reader.fieldnames:
                columns_to_keep = filter_columns(list(reader.fieldnames), endpoint)

            for row in reader:
                # Filter columns if needed
                if columns_to_keep:
                    filtered_row = {k: v for k, v in row.items() if k in columns_to_keep}
                else:
                    filtered_row = row

                # Clean empty strings to None
                cleaned_row = {k: (v.strip() if v and v.strip() else None) for k, v in filtered_row.items()}
                yield cleaned_row
                rows_loaded += 1

            logger.info(f"{endpoint} page {page}: loaded {rows_loaded} rows")

            if rows_loaded < size:
                logger.info(f"{endpoint}: got {rows_loaded} < {size}, last page reached")
                break

            page += 1
            time.sleep(0.5)  # Rate limit

        except Exception as e:
            logger.error(f"Error fetching {endpoint} page {page}: {e}")
            break


def fetch_csv_batches(endpoint: str, page_size: int, batch_size: Optional[int] = None) -> Iterator[List[Dict[str, Any]]]:
    """Retourne un iterateur sur des batches de données pour un endpoint donné"""

    batch_size = batch_size or page_size
    iterator = fetch_csv_data(endpoint, size=page_size)

    while True:
        batch = list(itertools.islice(iterator, batch_size))
        if not batch:
            break
        yield batch


def iter_sandre_batches(resource_name: str, batch_size: Optional[int] = None) -> Iterator[List[Dict[str, Any]]]:
    """Itère sur les batches pour une ressource SANDRE donnée"""

    config = SANDRE_RESOURCE_CONFIG.get(resource_name)
    if config is None:
        raise KeyError(f"Unknown SANDRE resource: {resource_name}")

    return fetch_csv_batches(config["endpoint"], config["page_size"], batch_size=batch_size)


@dlt.source
def sandre_raw():
    """Source pour tous les référentiels SANDRE"""

    @dlt.resource(name="sandre_parametres", write_disposition="replace")
    def parametres() -> Iterator[Dict[str, Any]]:
        """Paramètres physico-chimiques"""
        logger.info("Loading SANDRE parametres...")
        return fetch_csv_data("parametre", size=2000)

    @dlt.resource(name="sandre_unites", write_disposition="replace")
    def unites() -> Iterator[Dict[str, Any]]:
        """Unités de mesure"""
        logger.info("Loading SANDRE unites...")
        return fetch_csv_data("unite", size=1000)

    @dlt.resource(name="sandre_methodes", write_disposition="replace")
    def methodes() -> Iterator[Dict[str, Any]]:
        """Méthodes d'analyse"""
        logger.info("Loading SANDRE methodes...")
        return fetch_csv_data("methode", size=1000)

    @dlt.resource(name="sandre_supports", write_disposition="replace")
    def supports() -> Iterator[Dict[str, Any]]:
        """Supports de prélèvement"""
        logger.info("Loading SANDRE supports...")
        return fetch_csv_data("support", size=100)

    @dlt.resource(name="sandre_fractions", write_disposition="replace")
    def fractions() -> Iterator[Dict[str, Any]]:
        """Fractions analysées"""
        logger.info("Loading SANDRE fractions analysees...")
        return fetch_csv_data("fractionanalysee", size=50)

    @dlt.resource(name="sandre_communes", write_disposition="replace")
    def communes() -> Iterator[Dict[str, Any]]:
        """Communes françaises"""
        logger.info("Loading SANDRE communes...")
        return fetch_csv_data("commune", size=5000)

    @dlt.resource(name="sandre_departements", write_disposition="replace")
    def departements() -> Iterator[Dict[str, Any]]:
        """Départements"""
        logger.info("Loading SANDRE departements...")
        return fetch_csv_data("departement", size=200)

    @dlt.resource(name="sandre_regions", write_disposition="replace")
    def regions() -> Iterator[Dict[str, Any]]:
        """Régions"""
        logger.info("Loading SANDRE regions...")
        return fetch_csv_data("region", size=50)

    @dlt.resource(name="sandre_intervenants", write_disposition="replace")
    def intervenants() -> Iterator[Dict[str, Any]]:
        """Organismes et laboratoires"""
        logger.info("Loading SANDRE intervenants...")
        return fetch_csv_data("intervenant", size=5000)

    @dlt.resource(name="sandre_masses_eau_sout", write_disposition="replace")
    def masses_eau_souterraines() -> Iterator[Dict[str, Any]]:
        """Masses d'eau souterraines"""
        logger.info("Loading SANDRE masses eau souterraines...")
        return fetch_csv_data("massedeausouterraine", size=1000)

    @dlt.resource(name="sandre_masses_eau_surf", write_disposition="replace")
    def masses_eau_surface() -> Iterator[Dict[str, Any]]:
        """Masses d'eau de surface (rivières)"""
        logger.info("Loading SANDRE masses eau surface...")
        return fetch_csv_data("massedeau", size=5000)

    @dlt.resource(name="sandre_cours_eau", write_disposition="replace")
    def cours_eau() -> Iterator[Dict[str, Any]]:
        """Cours d'eau"""
        logger.info("Loading SANDRE cours eau...")
        return fetch_csv_data("courseau", size=10000)

    @dlt.resource(name="sandre_bassins", write_disposition="replace")
    def bassins() -> Iterator[Dict[str, Any]]:
        """Bassins versants"""
        logger.info("Loading SANDRE bassins versants...")
        return fetch_csv_data("circonscriptionbassin", size=500)

    @dlt.resource(name="sandre_milieux", write_disposition="replace")
    def milieux() -> Iterator[Dict[str, Any]]:
        """Milieux de prélèvement"""
        logger.info("Loading SANDRE milieux...")
        return fetch_csv_data("milieu", size=50)

    return [
        parametres,
        unites,
        methodes,
        supports,
        fractions,
        communes,
        departements,
        regions,
        intervenants,
        masses_eau_souterraines,
        masses_eau_surface,
        cours_eau,
        bassins,
        milieux
    ]