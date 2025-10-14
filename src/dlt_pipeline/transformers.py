"""
DLT Transformers pour validation et nettoyage des donnees Hub'Eau.

PRINCIPE: Rejeter les donnees invalides, jamais les corrompre.

Architecture:
- Couche 1: Hub'Eau API (extraction)
- Couche 2: DLT Pipeline (transformation) <- CE FICHIER
- Couche 3: Dagster (orchestration)

REFACTORED: Utilise DLT transformer natif avec opérateur pipe |
"""
import logging
import re
from typing import Any, Dict, Iterator, List, Optional
from datetime import datetime

import dlt
from dlt.common.typing import TDataItem

logger = logging.getLogger(__name__)


def get_transformer(name: str):
    """
    Récupère un transformer par son nom.

    Args:
        name: Nom du transformer

    Returns:
        Fonction transformer

    Raises:
        ValueError: Si le transformer n'existe pas
    """
    transformers = {
        "validate_primary_keys": validate_primary_keys,
        "normalize_dates": normalize_dates,
        "validate_coordinates": validate_coordinates,
        "reject_duplicates": reject_duplicates,
        "clean_text": clean_text,
        "convert_types": convert_types
    }

    if name not in transformers:
        raise ValueError(
            f"Unknown transformer: {name}. "
            f"Available: {', '.join(transformers.keys())}"
        )

    return transformers[name]


# ====================================
# TRANSFORMERS DLT NATIFS (item-by-item)
# ====================================

@dlt.transformer
def validate_primary_keys(
    item: TDataItem,
    primary_keys: List[str],
    api_name: str = "unknown"
):
    """
    Valide que les cles primaires sont non-NULL.

    PRINCIPE CORRECT:
    - Les records avec primary key NULL sont REJETES
    - Les rejets sont logges pour investigation
    - Aucune donnee n'est inventee ou substituee

    Args:
        item: Record DLT individuel
        primary_keys: Liste des cles primaires
        api_name: Nom de l'API pour logging

    Yields:
        Record seulement si toutes les primary keys sont valides
    """
    # Verifier toutes les primary keys
    null_keys = [pk for pk in primary_keys if item.get(pk) is None]

    if null_keys:
        # REJETER le record en ne le yieldant pas
        logger.debug(
            f"{api_name}: Record rejete (primary keys NULL: {null_keys})"
        )
        return  # Ne rien yielder

    # Record valide
    yield item


@dlt.transformer
def normalize_dates(
    item: TDataItem,
    fields: Optional[List[str]] = None
):
    """
    Normalise les formats de dates en ISO 8601 pour un item.

    Les dates invalides sont mises a NULL (pas de corruption).

    Args:
        item: Record DLT individuel
        fields: Champs date a normaliser (auto-detecte si None)

    Yields:
        Record avec dates normalisees
    """
    date_patterns = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
    ]

    # Auto-detection des champs date
    date_fields = fields
    if date_fields is None:
        date_fields = [
            k for k in item.keys()
            if "date" in k.lower() or "timestamp" in k.lower()
        ]

    for field in date_fields:
        if field not in item or item[field] is None:
            continue

        value = item[field]

        # Deja une datetime?
        if isinstance(value, datetime):
            item[field] = value.isoformat()
            continue

        # Tenter parsing
        parsed = False
        for pattern in date_patterns:
            try:
                dt = datetime.strptime(str(value), pattern)
                item[field] = dt.isoformat()
                parsed = True
                break
            except (ValueError, TypeError):
                continue

        # Si parsing echoue, mettre a NULL (pas de corruption)
        if not parsed:
            logger.debug(
                f"Date invalide '{value}' -> NULL (champ: {field})"
            )
            item[field] = None

    yield item


@dlt.transformer
def validate_coordinates(
    item: TDataItem,
    lat_field: str = "latitude",
    lon_field: str = "longitude"
):
    """
    Valide les coordonnees geographiques pour un item.

    Les coordonnees hors range sont mises a NULL (pas de correction inventee).

    Ranges valides:
    - Latitude: [-90, 90]
    - Longitude: [-180, 180]

    Args:
        item: Record DLT individuel
        lat_field: Nom du champ latitude
        lon_field: Nom du champ longitude

    Yields:
        Record avec coordonnees validees
    """
    if lat_field in item and item[lat_field] is not None:
        try:
            lat = float(item[lat_field])

            # Valider le range
            if not (-90 <= lat <= 90):
                logger.debug(f"Latitude hors range: {lat} -> NULL")
                item[lat_field] = None

        except (ValueError, TypeError):
            logger.debug(f"Latitude non-numerique -> NULL")
            item[lat_field] = None

    if lon_field in item and item[lon_field] is not None:
        try:
            lon = float(item[lon_field])

            # Valider le range
            if not (-180 <= lon <= 180):
                logger.debug(f"Longitude hors range: {lon} -> NULL")
                item[lon_field] = None

        except (ValueError, TypeError):
            logger.debug(f"Longitude non-numerique -> NULL")
            item[lon_field] = None

    yield item


def create_reject_duplicates_transformer(key_fields: List[str], api_name: str = "unknown"):
    """
    Factory qui crée un transformer reject_duplicates avec state partagé.

    Ce pattern est nécessaire car le transformer doit maintenir un state
    (les clés déjà vues) entre tous les items du stream.

    Args:
        key_fields: Champs formant la cle unique
        api_name: Nom de l'API pour logging

    Returns:
        Transformer DLT configuré avec state partagé
    """
    # State partagé entre tous les items
    seen_keys = set()

    @dlt.transformer
    def reject_duplicates_transformer(item: TDataItem):
        """
        Rejette les doublons bases sur les key_fields.

        Garde le premier record rencontre, rejette les suivants.
        """
        nonlocal seen_keys

        # Construire la cle
        key = tuple(item.get(f) for f in key_fields)

        if key in seen_keys:
            # REJETER le doublon
            logger.debug(f"{api_name}: Doublon rejete (key: {key})")
            return  # Ne rien yielder

        # Marquer comme vu
        seen_keys.add(key)
        yield item

    return reject_duplicates_transformer


def reject_duplicates(
    item: TDataItem,
    key_fields: List[str],
    api_name: str = "unknown"
):
    """
    Transformer reject_duplicates (version simple sans state).

    ATTENTION: Cette version ne peut pas tracker les doublons correctement
    car elle ne maintient pas de state entre les items!

    Utilisez create_reject_duplicates_transformer() à la place pour
    une détection correcte des doublons.

    Cette fonction existe uniquement pour la compatibilité avec get_transformer().
    """
    logger.warning(
        f"{api_name}: reject_duplicates appelé sans state - "
        "doublons non détectés! Utilisez create_reject_duplicates_transformer()"
    )
    yield item


@dlt.transformer
def clean_text(
    item: TDataItem,
    fields: Optional[List[str]] = None,
    api_name: str = "unknown"
):
    """
    Nettoie les champs texte (trim, normalisation) pour un item.

    Operations:
    - Strip whitespace
    - Normalise les espaces multiples
    - Supprime les caractères de contrôle

    Args:
        item: Record DLT individuel
        fields: Champs texte à nettoyer (auto-détecte si None)
        api_name: Nom de l'API pour logging

    Yields:
        Record avec textes nettoyés
    """
    # Auto-détection des champs texte
    if fields is None:
        text_fields = [
            k for k, v in item.items()
            if isinstance(v, str)
        ]
    else:
        text_fields = fields

    for field in text_fields:
        if field not in item or item[field] is None:
            continue

        value = item[field]

        if isinstance(value, str):
            # Strip whitespace
            cleaned = value.strip()

            # Normaliser espaces multiples
            cleaned = re.sub(r'\s+', ' ', cleaned)

            # Supprimer caractères de contrôle
            cleaned = re.sub(r'[\x00-\x1F\x7F]', '', cleaned)

            # Mettre à NULL si vide après nettoyage
            if not cleaned:
                item[field] = None
            elif cleaned != value:
                item[field] = cleaned

    yield item


@dlt.transformer
def convert_types(
    item: TDataItem,
    conversions: Optional[Dict[str, str]] = None,
    api_name: str = "unknown"
):
    """
    Convertit les types de données selon un mapping pour un item.

    Types supportés:
    - int, float, str, bool
    - date, datetime

    Args:
        item: Record DLT individuel
        conversions: Mapping {field: type} ex: {"count": "int", "active": "bool"}
        api_name: Nom de l'API pour logging

    Yields:
        Record avec types convertis
    """
    if not conversions:
        # Pas de conversion, passer directement
        yield item
        return

    for field, target_type in conversions.items():
        if field not in item or item[field] is None:
            continue

        value = item[field]

        try:
            if target_type == "int":
                item[field] = int(float(value))  # float intermediaire pour "123.0"

            elif target_type == "float":
                item[field] = float(value)

            elif target_type == "str":
                item[field] = str(value)

            elif target_type == "bool":
                # Mapping flexible pour bool
                if isinstance(value, bool):
                    pass  # Déjà bool
                elif isinstance(value, str):
                    item[field] = value.lower() in ["true", "1", "yes", "oui"]
                else:
                    item[field] = bool(value)

            elif target_type in ["date", "datetime"]:
                # Si déjà datetime, garder
                if isinstance(value, datetime):
                    pass
                else:
                    # Tenter parsing
                    from datetime import datetime as dt
                    if isinstance(value, str):
                        item[field] = dt.fromisoformat(value)

        except (ValueError, TypeError) as e:
            logger.debug(
                f"Conversion échouée pour {field}: {value} -> {target_type}"
            )
            item[field] = None

    yield item
