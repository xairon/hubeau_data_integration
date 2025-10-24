"""
Script pour nettoyer les configs YAML Hub'Eau et ajouter batch_size

Supprime les champs obsolètes :
- resource.columns (détection automatique depuis CSV)
- resource.write_disposition (déterminé dynamiquement)
- source.max_table_nesting (inutile pour CSV)
- transformers (non implémenté)
- destinations.postgres.dataset_name (utilise HUBEAU_SCHEMA env var)

Ajoute performance.batch_size selon la resource (mappé depuis le code actuel)
"""

import yaml
from pathlib import Path
from typing import Dict, Any

# Mapping resource_name → batch_size (depuis hubeau_assets.py lignes 115-160)
BATCH_SIZES = {
    # ULTRA_CRITICAL_OOM_RESOURCES → 1,000
    "quality_groundwater_stations": 1000,
    "hydrobio_stations": 1000,

    # VERY_LARGE_RESOURCES → 5,000
    "prelevements_points": 5000,
    "prelevements_ouvrages": 5000,

    # LARGE_RESOURCES → 8,000
    "piezometry_stations": 8000,
    "quality_rivers_stations": 8000,
    "hydrobio_indices": 8000,
    "hydrobio_taxons": 8000,
    "quality_rivers_analyses": 8000,
    "prelevements_chroniques": 8000,
    "hydrometry_obs_elab": 8000,

    # Tous les autres → 50,000 (défaut)
    # (pas besoin de les lister, on applique par défaut)
}


def clean_yaml_config(config: Dict[str, Any], resource_name: str) -> Dict[str, Any]:
    """
    Nettoie une config YAML en gardant SEULEMENT les champs utilisés

    Structure finale:
        resource:
            name, endpoint, base_url, primary_key
        extraction:
            default_params
        performance:
            parallelism, chunk_size, batch_size, retry_times, retry_delay, rate_limit
    """
    cleaned = {}

    # 1. Resource section (gardé minimal)
    if 'resource' in config:
        cleaned['resource'] = {
            'name': config['resource']['name'],
            'endpoint': config['resource']['endpoint'],
            'base_url': config['resource']['base_url'],
            'primary_key': config['resource']['primary_key']
        }

    # 2. Extraction section
    if 'extraction' in config:
        cleaned['extraction'] = {
            'default_params': config['extraction'].get('default_params', {})
        }

    # 3. Performance section (+ batch_size)
    if 'performance' in config:
        perf = config['performance']

        # Déterminer batch_size depuis mapping
        batch_size = BATCH_SIZES.get(resource_name, 50000)  # Défaut: 50k

        cleaned['performance'] = {
            'parallelism': perf.get('parallelism', 5),
            'chunk_size': perf.get('chunk_size', 10000),
            'batch_size': batch_size,  # ← NOUVEAU !
            'retry_times': perf.get('retry_times', 3),
            'retry_delay': perf.get('retry_delay', 2.0),
            'rate_limit': perf.get('rate_limit', 2.0)
        }

    return cleaned


def main():
    """Nettoie tous les YAML dans configs/hubeau/"""
    configs_dir = Path("configs/hubeau")

    if not configs_dir.exists():
        print(f"❌ Répertoire {configs_dir} introuvable")
        return

    yaml_files = list(configs_dir.glob("*.yml"))

    if not yaml_files:
        print(f"❌ Aucun fichier YAML trouvé dans {configs_dir}")
        return

    print(f"Nettoyage de {len(yaml_files)} fichiers YAML...")

    for yaml_file in yaml_files:
        resource_name = yaml_file.stem  # Nom sans extension

        print(f"\n[{yaml_file.name}]")

        # Charger config existante
        with open(yaml_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        # Nettoyer
        cleaned_config = clean_yaml_config(config, resource_name)

        # Sauvegarder
        with open(yaml_file, 'w', encoding='utf-8') as f:
            yaml.dump(
                cleaned_config,
                f,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True
            )

        batch_size = cleaned_config['performance']['batch_size']
        print(f"  OK - batch_size={batch_size:,}")

    print(f"\n{len(yaml_files)} fichiers nettoyes avec succes!")
    print("\nResume batch sizes:")
    print("  - ULTRA_CRITICAL (1k):  quality_groundwater_stations, hydrobio_stations")
    print("  - VERY_LARGE (5k):      prelevements_points, prelevements_ouvrages")
    print("  - LARGE (8k):           piezometry_stations, quality_rivers_stations, hydrobio_*, ...")
    print("  - STANDARD (50k):       Tous les autres")


if __name__ == "__main__":
    main()
