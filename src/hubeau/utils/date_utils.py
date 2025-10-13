"""
Module d'utilitaires pour la gestion des dates dans le pipeline Hub'Eau.
Factorise les fonctions communes de manipulation de dates.
"""

from datetime import date, datetime, timedelta
import calendar
from typing import Iterator, Tuple, Optional


def limit_to_partition_year(start: date, end_candidate: date) -> date:
    """
    Limite la date de fin à l'année de la partition si on commence au 1er janvier.

    Cette fonction est utilisée pour respecter les limites de partition annuelle
    lors du slicing des données. Si la date de début est le 1er janvier,
    la date de fin ne doit pas dépasser le 31 décembre de la même année.

    Args:
        start: Date de début
        end_candidate: Date de fin candidate

    Returns:
        Date de fin limitée à l'année si nécessaire
    """
    if start.month == 1 and start.day == 1:
        end_of_year = date(start.year, 12, 31)
        return min(end_candidate, end_of_year)
    return end_candidate


def month_windows(start: date, end: date) -> Iterator[Tuple[date, date]]:
    """
    Génère des fenêtres mensuelles entre deux dates.

    Cette fonction découpe une période en fenêtres mensuelles complètes,
    utilisées pour le slicing mensuel des données.

    Args:
        start: Date de début
        end: Date de fin

    Yields:
        Tuples (début_mois, fin_mois) pour chaque mois dans la période
    """
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


def daterange(start_date: date, end_date: date) -> Iterator[date]:
    """
    Génère une séquence de dates entre start_date et end_date inclus.

    Args:
        start_date: Date de début
        end_date: Date de fin

    Yields:
        Chaque date entre start et end inclus
    """
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def get_year_range(year: int) -> Tuple[date, date]:
    """
    Retourne le premier et dernier jour d'une année.

    Args:
        year: Année

    Returns:
        Tuple (1er janvier, 31 décembre) de l'année
    """
    return date(year, 1, 1), date(year, 12, 31)


def get_month_range(year: int, month: int) -> Tuple[date, date]:
    """
    Retourne le premier et dernier jour d'un mois.

    Args:
        year: Année
        month: Mois (1-12)

    Returns:
        Tuple (premier jour, dernier jour) du mois
    """
    first_day = date(year, month, 1)
    last_day = date(year, month, calendar.monthrange(year, month)[1])
    return first_day, last_day


def parse_date_flexible(date_str: str) -> Optional[date]:
    """
    Parse une date avec plusieurs formats possibles.

    Args:
        date_str: Chaîne de date à parser

    Returns:
        Objet date ou None si le parsing échoue
    """
    formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y%m%d",
        "%Y"  # Année seule -> 1er janvier
    ]

    for fmt in formats:
        try:
            parsed = datetime.strptime(date_str, fmt).date()
            # Si année seule, retourner le 1er janvier
            if fmt == "%Y":
                return date(parsed.year, 1, 1)
            return parsed
        except ValueError:
            continue

    return None


def format_month_key(year: int, month: int) -> str:
    """
    Formate une clé de mois au format YYYY-MM.

    Args:
        year: Année
        month: Mois (1-12)

    Returns:
        Chaîne au format "YYYY-MM"
    """
    return f"{year}-{month:02d}"


def get_months_in_year(year: int) -> list[str]:
    """
    Retourne la liste de tous les mois d'une année au format YYYY-MM.

    Args:
        year: Année

    Returns:
        Liste des 12 mois ["YYYY-01", "YYYY-02", ..., "YYYY-12"]
    """
    return [format_month_key(year, m) for m in range(1, 13)]


def is_date_in_range(check_date: date, start: date, end: date) -> bool:
    """
    Vérifie si une date est dans une plage (bornes incluses).

    Args:
        check_date: Date à vérifier
        start: Début de la plage
        end: Fin de la plage

    Returns:
        True si la date est dans la plage
    """
    return start <= check_date <= end