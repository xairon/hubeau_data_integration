"""
Script pour migrer les anciennes configs vers le nouveau format DLT natif
"""
import yaml
from pathlib import Path
import shutil
from datetime import datetime


def infer_columns_from_endpoint(api_name: str, endpoint: str) -> dict:
    """
    Infère les colonnes principales selon l'API et l'endpoint.
    """
    # Colonnes communes à toutes les APIs
    common_columns = {
        "code_station": {
            "data_type": "text",
            "nullable": False
        },
        "libelle_station": {
            "data_type": "text",
            "nullable": True
        },
        "latitude": {
            "data_type": "double",
            "nullable": True
        },
        "longitude": {
            "data_type": "double",
            "nullable": True
        }
    }

    # Colonnes spécifiques par type d'endpoint
    if "stations" in endpoint:
        return common_columns

    elif "chroniques" in endpoint or "observations" in endpoint:
        return {
            **common_columns,
            "date_obs": {
                "data_type": "timestamp",
                "nullable": False
            },
            "resultat": {
                "data_type": "double",
                "nullable": True
            },
            "code_parametre": {
                "data_type": "text",
                "nullable": True
            }
        }

    elif "analyses" in endpoint:
        return {
            "code_prelevement": {
                "data_type": "text",
                "nullable": False
            },
            "date_prelevement": {
                "data_type": "timestamp",
                "nullable": True
            },
            "code_parametre": {
                "data_type": "text",
                "nullable": True
            },
            "resultat": {
                "data_type": "double",
                "nullable": True
            },
            "unite": {
                "data_type": "text",
                "nullable": True
            }
        }

    elif "operations" in endpoint:
        return {
            "code_operation": {
                "data_type": "text",
                "nullable": False
            },
            "date_operation": {
                "data_type": "timestamp",
                "nullable": True
            },
            "code_station": {
                "data_type": "text",
                "nullable": True
            }
        }

    else:
        # Colonnes minimales par défaut
        return {
            "id": {
                "data_type": "text",
                "nullable": True
            }
        }


def determine_write_disposition(endpoint: str) -> str:
    """
    Détermine la stratégie d'écriture selon l'endpoint.
    """
    if "stations" in endpoint or "sites" in endpoint:
        return "replace"  # Les stations sont des référentiels complets
    else:
        return "merge"  # Les chroniques/analyses sont incrémentales


def determine_slicing_mode(old_config: dict) -> str:
    """
    Détermine le mode de slicing optimal.
    """
    slicing_mode = old_config.get("slicing_mode", "global")

    # Mapping des anciens modes vers les nouveaux
    mode_mapping = {
        "station_month_chunked": "station_month_chunked",
        "dept_datetime": "dept_datetime",
        "datetime": "datetime",
        "dept": "dept",
        "global": "global"
    }

    return mode_mapping.get(slicing_mode, "global")


def get_date_fields(api_name: str, endpoint: str) -> list:
    """
    Retourne les champs de date à normaliser selon l'endpoint.
    """
    date_fields_map = {
        "stations": ["date_ouverture", "date_fermeture", "date_maj"],
        "chroniques": ["date_obs", "date_debut", "date_fin"],
        "observations": ["date_obs", "date_observation"],
        "analyses": ["date_prelevement", "date_analyse"],
        "operations": ["date_operation", "date_debut", "date_fin"],
        "campagnes": ["date_debut", "date_fin"],
        "conditions": ["date_observation"]
    }

    for key, fields in date_fields_map.items():
        if key in endpoint:
            return fields

    return ["date_obs"]  # Par défaut


def migrate_config(old_config_path: Path) -> dict:
    """
    Migre une config vers le nouveau format DLT natif.
    """
    with open(old_config_path, encoding='utf-8') as f:
        old = yaml.safe_load(f)

    # Extraire le nom de l'endpoint depuis le nom du fichier
    # Format: api_name_endpoint.yml
    file_stem = old_config_path.stem
    api_name = old.get("api_name", file_stem.split("_")[0])

    # Construire le nom de la resource
    endpoint_clean = old.get("endpoint", "").strip("/").replace("/", "_")
    if not endpoint_clean:
        # Essayer de deviner depuis le nom du fichier
        parts = file_stem.split("_")
        endpoint_clean = "_".join(parts[1:]) if len(parts) > 1 else "data"

    resource_name = f"{api_name}_{endpoint_clean}" if endpoint_clean else api_name

    # Déterminer les transformers appropriés
    transformers = ["validate_primary_keys"]

    date_fields = get_date_fields(api_name, endpoint_clean)
    if date_fields:
        transformers.append({
            "normalize_dates": {
                "fields": date_fields
            }
        })

    # Ajouter validation des coordonnées si pertinent
    if "stations" in endpoint_clean or "sites" in endpoint_clean:
        transformers.append({
            "validate_coordinates": {
                "lat_field": "latitude",
                "lon_field": "longitude"
            }
        })

    # Déduire le page_size des params existants
    page_size = old.get("params", {}).get("size", 20000)
    if "page_size" in old:
        page_size = old["page_size"]

    # Nouveau format complet
    new = {
        "source": {
            "name": api_name,
            "max_table_nesting": 2
        },
        "resource": {
            "name": resource_name,
            "endpoint": old.get("endpoint", f"/{endpoint_clean}"),
            "base_url": old.get("base_url", f"https://hubeau.eaufrance.fr/api/v1/{api_name}"),
            "primary_key": old.get("primary_keys", old.get("primary_key", [])),
            "write_disposition": old.get("write_disposition", determine_write_disposition(endpoint_clean)),
            "columns": infer_columns_from_endpoint(api_name, endpoint_clean)
        },
        "extraction": {
            "slicing_mode": determine_slicing_mode(old),
            "pagination": {
                "type": old.get("pagination_type", "page_based"),
                "page_size": page_size,
                "max_pages": old.get("max_pages")
            },
            "default_params": old.get("params", {})
        },
        "performance": {
            "parallelism": old.get("parallelism", 5),
            "chunk_size": old.get("chunk_size", 10000),
            "retry_times": old.get("retry_times", 3),
            "retry_delay": old.get("retry_delay", 2.0),
            "rate_limit": old.get("rate_limit", 2.0)
        },
        "transformers": transformers,
        "destinations": {
            "postgres": {
                "dataset_name": "bronze",
                "schema_name": api_name,
                "create_indexes": True
            },
            "filesystem": {
                "bucket_url": "s3://hubeau-bronze",
                "file_format": "parquet",
                "layout": f"{{table_name}}/year={{year}}/{{load_id}}.parquet"
            },
            "duckdb": {
                "database": f"data/{api_name}.duckdb"
            }
        }
    }

    # Préserver les champs custom s'ils existent
    if "custom_params" in old:
        new["extraction"]["custom_params"] = old["custom_params"]

    if "date_field" in old:
        new["extraction"]["date_field"] = old["date_field"]

    if "sort_field" in old:
        new["extraction"]["sort_field"] = old["sort_field"]

    return new


def validate_new_config(config: dict) -> list:
    """
    Valide le nouveau format de configuration.
    """
    errors = []

    # Sections requises
    required_sections = ["source", "resource", "extraction"]
    for section in required_sections:
        if section not in config:
            errors.append(f"Missing required section: {section}")

    # Champs requis dans resource
    if "resource" in config:
        required_resource_fields = ["name", "endpoint", "base_url"]
        for field in required_resource_fields:
            if field not in config["resource"]:
                errors.append(f"Missing required field in resource: {field}")

    # Validation du slicing_mode
    if "extraction" in config:
        valid_slicing_modes = ["global", "datetime", "dept", "station_month_chunked", "dept_datetime"]
        slicing_mode = config["extraction"].get("slicing_mode")
        if slicing_mode and slicing_mode not in valid_slicing_modes:
            errors.append(f"Invalid slicing_mode: {slicing_mode}")

    return errors


def main():
    """
    Migre toutes les configurations Hub'Eau vers le nouveau format.
    """
    config_dir = Path("configs/hubeau")

    if not config_dir.exists():
        print(f"Error: Config directory {config_dir} not found!")
        return 1

    # Créer un dossier de backup avec timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = config_dir.parent / f"hubeau_backup_{timestamp}"

    print(f"Creating backup in {backup_dir}...")
    shutil.copytree(config_dir, backup_dir)

    # Lister tous les fichiers YAML
    yaml_files = list(config_dir.glob("*.yml"))
    print(f"Found {len(yaml_files)} YAML files to migrate")

    success_count = 0
    error_count = 0

    for yaml_file in yaml_files:
        try:
            print(f"\nMigrating {yaml_file.name}...")

            # Migrer la configuration
            new_config = migrate_config(yaml_file)

            # Valider
            errors = validate_new_config(new_config)
            if errors:
                print(f"  [WARNING] Validation errors for {yaml_file.name}:")
                for error in errors:
                    print(f"    - {error}")
                error_count += 1
                continue

            # Écrire la nouvelle configuration
            with open(yaml_file, 'w', encoding='utf-8') as f:
                yaml.dump(new_config, f, default_flow_style=False, sort_keys=False, width=120, allow_unicode=True)

            print(f"  [OK] Successfully migrated {yaml_file.name}")
            success_count += 1

        except Exception as e:
            print(f"  [ERROR] Error migrating {yaml_file.name}: {e}")
            error_count += 1

    print(f"\n{'='*50}")
    print(f"Migration completed:")
    print(f"  [SUCCESS] {success_count} files migrated")
    print(f"  [ERRORS] {error_count} files failed")
    print(f"  [BACKUP] Saved to: {backup_dir}")

    if error_count > 0:
        print(f"\n[WARNING] Some files had errors. Check the backup and fix manually if needed.")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())