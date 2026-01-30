"""
Données de fallback pour les nomenclatures Sandre (entités hydrogéologiques).
Utilisées lorsque l'API Sandre NSA est indisponible.
Source: Sandre PRL / SAQ 2002-1, doc BDLISA.
"""

from typing import List, Tuple

REF_NATURE_EH: List[Tuple[str, str]] = [
    ("0", "Inconnue"),
    ("1", "Grand système aquifère"),
    ("2", "Grand domaine hydrogéologique"),
    ("3", "Système aquifère"),
    ("4", "Domaine hydrogéologique"),
    ("5", "Unité aquifère"),
    ("6", "Unité semi-perméable"),
    ("7", "Unité imperméable"),
    ("12", "Grand système multicouche"),
    ("X", "Non renseigné"),
]
REF_ETAT_EH: List[Tuple[str, str]] = [
    ("0", "Non renseigné"),
    ("1", "Entité hydrogéologique à nappe captive"),
    ("2", "Entité hydrogéologique à nappe libre"),
    ("3", "Entité hydrogéologique à parties libres et captives"),
    ("4", "Entité hydrogéologique alternativement libre puis captive"),
    ("5", "Entité hydrogéologique partiellement captive"),
    ("6", "Non défini (hors nomenclature SAQ 2002-1)"),
    ("X", "Non renseigné"),
]
REF_THEME_EH: List[Tuple[str, str]] = [
    ("0", "Inconnu"),
    ("1", "Alluvial"),
    ("2", "sédimentaire"),
    ("3", "Socle"),
    ("4", "Intensément plissés de montagne"),
    ("5", "Volcanisme"),
    ("X", "Non renseigné"),
]
REF_MILIEU_EH: List[Tuple[str, str]] = [
    ("0", "Inconnu"),
    ("1", "Poreux"),
    ("2", "Fissuré"),
    ("3", "Karstique"),
    ("4", "Poreux et fissuré"),
    ("5", "Fissuré et karstique"),
    ("6", "Multicouche"),
    ("7", "Poreux et karstique"),
    ("8", "Mixte"),
    ("9", "Autre"),
    ("10", "Non applicable"),
    ("X", "Non renseigné"),
]
REF_ORIGINE_EH: List[Tuple[str, str]] = [
    ("1", "Forte potentialité aquifère"),
    ("2", "Potentialité aquifère moyenne"),
    ("3", "Faible potentialité aquifère"),
    ("4", "Nulle ou très faible potentialité"),
    ("X", "Non renseigné"),
]
