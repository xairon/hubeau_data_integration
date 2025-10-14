"""
DLT Transformers pour validation et nettoyage des donnees Hub'Eau.

PRINCIPE: Rejeter les donnees invalides, jamais les corrompre.

Architecture:
- Couche 1: Hub'Eau API (extraction)
- Couche 2: DLT Pipeline (transformation) <- CE FICHIER
- Couche 3: Dagster (orchestration)
"""
import logging
from typing import Any, Dict, Iterator, List, Optional
from datetime import datetime

import dlt
from dlt.common.typing import TDataItem

logger = logging.getLogger(__name__)


def apply_transformers(
    data: Iterator[TDataItem],
    transformer_configs: List,
    api_name: str = "unknown"
) -> Iterator[TDataItem]:
    """
    Applique une chaîne de transformers selon la config YAML.

    Args:
        data: Données à transformer
        transformer_configs: Liste des transformers à appliquer
        api_name: Nom de l'API pour logging

    Returns:
        Iterator avec transformations appliquées

    Example config YAML:
        transformers:
          - validate_primary_keys
          - normalize_dates:
              fields: [date_debut, date_fin]
          - validate_coordinates:
              lat_field: latitude
              lon_field: longitude
    """
    result = data

    for transformer_config in transformer_configs:
        if isinstance(transformer_config, str):
            # Transformer sans paramètres
            transformer_name = transformer_config
            params = {}
        else:
            # Transformer avec paramètres
            transformer_name = list(transformer_config.keys())[0]
            params = transformer_config[transformer_name]

        # Récupérer et appliquer le transformer
        transformer_func = get_transformer(transformer_name)

        # Ajouter api_name aux params si le transformer l'accepte
        if transformer_name in ["validate_primary_keys", "reject_duplicates"]:
            params["api_name"] = api_name

        # Appliquer le transformer
        result = transformer_func(result, **params)

    return result


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


@dlt.transformer(name="validate_primary_keys")
def validate_primary_keys(
    items: Iterator[TDataItem],
    primary_keys: List[str],
    api_name: str = "unknown"
) -> Iterator[TDataItem]:
    """
    Valide que les cles primaires sont non-NULL.
    
    PRINCIPE CORRECT:
    - Les records avec primary key NULL sont REJETES
    - Les rejets sont logges pour investigation
    - Aucune donnee n'est inventee ou substituee
    
    Args:
        items: Iterator de records DLT
        primary_keys: Liste des cles primaires
        api_name: Nom de l'API pour logging
        
    Yields:
        Seulement les records avec primary keys valides
    """
    valid_count = 0
    rejected_count = 0
    rejected_samples = []  # Garder 5 exemples pour debug
    
    for item in items:
        # Verifier toutes les primary keys
        null_keys = [pk for pk in primary_keys if item.get(pk) is None]
        
        if null_keys:
            # REJETER le record
            rejected_count += 1
            
            # Garder quelques exemples pour debug
            if len(rejected_samples) < 5:
                rejected_samples.append({
                    "null_keys": null_keys,
                    "record_sample": {k: item.get(k) for k in list(item.keys())[:5]}
                })
            
            continue  # Ne pas yielder
        
        # Record valide
        valid_count += 1
        yield item
    
    # Logging final
    if rejected_count > 0:
        logger.warning(
            f"{api_name}: {rejected_count} records rejetes "
            f"(primary keys NULL)"
        )
        for i, sample in enumerate(rejected_samples, 1):
            logger.debug(f"  Exemple {i}: {sample}")
    
    logger.info(
        f"{api_name}: {valid_count} records valides ingeres"
    )


@dlt.transformer(name="normalize_dates")
def normalize_dates(
    items: Iterator[TDataItem],
    fields: Optional[List[str]] = None
) -> Iterator[TDataItem]:
    """
    Normalise les formats de dates en ISO 8601.
    
    Les dates invalides sont mises a NULL (pas de corruption).
    
    Args:
        items: Iterator de records DLT
        fields: Champs date a normaliser (auto-detecte si None)
        
    Yields:
        Records avec dates normalisees
    """
    date_patterns = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
    ]
    
    normalized_count = 0
    failed_count = 0
    
    for item in items:
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
                normalized_count += 1
                continue
            
            # Tenter parsing
            parsed = False
            for pattern in date_patterns:
                try:
                    dt = datetime.strptime(str(value), pattern)
                    item[field] = dt.isoformat()
                    normalized_count += 1
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
                failed_count += 1
        
        yield item
    
    if normalized_count > 0 or failed_count > 0:
        logger.info(
            f"Dates: {normalized_count} normalisees, "
            f"{failed_count} mises a NULL (invalides)"
        )


@dlt.transformer(name="validate_coordinates")
def validate_coordinates(
    items: Iterator[TDataItem],
    lat_field: str = "latitude",
    lon_field: str = "longitude"
) -> Iterator[TDataItem]:
    """
    Valide les coordonnees geographiques.
    
    Les coordonnees hors range sont mises a NULL (pas de correction inventee).
    
    Ranges valides:
    - Latitude: [-90, 90]
    - Longitude: [-180, 180]
    
    Args:
        items: Iterator de records DLT
        lat_field: Nom du champ latitude
        lon_field: Nom du champ longitude
        
    Yields:
        Records avec coordonnees validees
    """
    invalid_count = 0
    
    for item in items:
        if lat_field in item and item[lat_field] is not None:
            try:
                lat = float(item[lat_field])
                
                # Valider le range
                if not (-90 <= lat <= 90):
                    logger.debug(
                        f"Latitude hors range: {lat} -> NULL"
                    )
                    item[lat_field] = None
                    invalid_count += 1
                    
            except (ValueError, TypeError):
                logger.debug(f"Latitude non-numerique -> NULL")
                item[lat_field] = None
                invalid_count += 1
        
        if lon_field in item and item[lon_field] is not None:
            try:
                lon = float(item[lon_field])
                
                # Valider le range
                if not (-180 <= lon <= 180):
                    logger.debug(
                        f"Longitude hors range: {lon} -> NULL"
                    )
                    item[lon_field] = None
                    invalid_count += 1
                    
            except (ValueError, TypeError):
                logger.debug(f"Longitude non-numerique -> NULL")
                item[lon_field] = None
                invalid_count += 1
        
        yield item
    
    if invalid_count > 0:
        logger.info(
            f"Coordonnees: {invalid_count} mises a NULL (invalides)"
        )


@dlt.transformer(name="reject_duplicates")
def reject_duplicates(
    items: Iterator[TDataItem],
    key_fields: List[str],
    api_name: str = "unknown"
) -> Iterator[TDataItem]:
    """
    Rejette les doublons bases sur les key_fields.
    
    Garde le premier record rencontre, rejette les suivants.
    
    Args:
        items: Iterator de records DLT
        key_fields: Champs formant la cle unique
        api_name: Nom de l'API pour logging
        
    Yields:
        Records uniques seulement
    """
    seen_keys = set()
    unique_count = 0
    duplicate_count = 0
    
    for item in items:
        # Construire la cle
        key = tuple(item.get(f) for f in key_fields)
        
        if key in seen_keys:
            # REJETER le doublon
            duplicate_count += 1
            continue
        
        # Marquer comme vu
        seen_keys.add(key)
        unique_count += 1
        yield item
    
    if duplicate_count > 0:
        logger.warning(
            f"{api_name}: {duplicate_count} doublons rejetes"
        )

    logger.info(f"{api_name}: {unique_count} records uniques")


@dlt.transformer(name="clean_text")
def clean_text(
    items: Iterator[TDataItem],
    fields: Optional[List[str]] = None,
    api_name: str = "unknown"
) -> Iterator[TDataItem]:
    """
    Nettoie les champs texte (trim, normalisation).

    Operations:
    - Strip whitespace
    - Normalise les espaces multiples
    - Supprime les caractères de contrôle

    Args:
        items: Iterator de records DLT
        fields: Champs texte à nettoyer (auto-détecte si None)
        api_name: Nom de l'API pour logging

    Yields:
        Records avec textes nettoyés
    """
    import re

    cleaned_count = 0

    for item in items:
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
                    cleaned_count += 1

        yield item

    if cleaned_count > 0:
        logger.info(f"{api_name}: {cleaned_count} champs texte nettoyés")


@dlt.transformer(name="convert_types")
def convert_types(
    items: Iterator[TDataItem],
    conversions: Optional[Dict[str, str]] = None,
    api_name: str = "unknown"
) -> Iterator[TDataItem]:
    """
    Convertit les types de données selon un mapping.

    Types supportés:
    - int, float, str, bool
    - date, datetime

    Args:
        items: Iterator de records DLT
        conversions: Mapping {field: type} ex: {"count": "int", "active": "bool"}
        api_name: Nom de l'API pour logging

    Yields:
        Records avec types convertis
    """
    if not conversions:
        # Pas de conversion, passer directement
        yield from items
        return

    converted_count = 0
    failed_count = 0

    for item in items:
        for field, target_type in conversions.items():
            if field not in item or item[field] is None:
                continue

            value = item[field]

            try:
                if target_type == "int":
                    item[field] = int(float(value))  # float intermediaire pour "123.0"
                    converted_count += 1

                elif target_type == "float":
                    item[field] = float(value)
                    converted_count += 1

                elif target_type == "str":
                    item[field] = str(value)
                    converted_count += 1

                elif target_type == "bool":
                    # Mapping flexible pour bool
                    if isinstance(value, bool):
                        pass  # Déjà bool
                    elif isinstance(value, str):
                        item[field] = value.lower() in ["true", "1", "yes", "oui"]
                    else:
                        item[field] = bool(value)
                    converted_count += 1

                elif target_type in ["date", "datetime"]:
                    # Si déjà datetime, garder
                    if isinstance(value, datetime):
                        pass
                    else:
                        # Tenter parsing (utilise normalize_dates en interne)
                        from datetime import datetime as dt
                        if isinstance(value, str):
                            item[field] = dt.fromisoformat(value)
                            converted_count += 1

            except (ValueError, TypeError) as e:
                logger.debug(
                    f"Conversion échouée pour {field}: {value} -> {target_type}"
                )
                item[field] = None
                failed_count += 1

        yield item

    if converted_count > 0:
        logger.info(
            f"{api_name}: {converted_count} conversions réussies, "
            f"{failed_count} échouées"
        )
