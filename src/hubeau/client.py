"""
Client Hub'Eau - Interface simple pour interagir avec les APIs Hub'Eau
"""
import yaml
from pathlib import Path
from typing import Iterator, Dict, Any, List, Optional
import dlt
from src.dlt_pipeline.sources import hubeau_source, load_config


class HubeauClient:
    """
    Client pour interagir avec les APIs Hub'Eau.

    Usage:
        client = HubeauClient()

        # Lister les APIs disponibles
        apis = client.list_apis()

        # Lister les endpoints d'une API
        endpoints = client.list_endpoints("hydrometry")

        # Récupérer des données
        data = client.get_data("hydrometry", "stations")
        for record in data:
            print(record)

        # Sauvegarder vers PostgreSQL
        client.save_to_postgres(data, "hydrometry_stations")
    """

    def __init__(self, config_dir: str = "configs/hubeau"):
        """
        Initialise le client Hub'Eau.

        Args:
            config_dir: Répertoire contenant les fichiers de configuration YAML
        """
        self.config_dir = Path(config_dir)

        if not self.config_dir.exists():
            raise FileNotFoundError(f"Config directory not found: {self.config_dir}")

        # Charger toutes les configurations disponibles
        self.configs = self._load_all_configs()

    def _load_all_configs(self) -> Dict[str, Dict]:
        """
        Charge toutes les configurations disponibles.

        Returns:
            Dict {config_name: config_dict}
        """
        configs = {}

        for config_file in self.config_dir.glob("*.yml"):
            try:
                with open(config_file, encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                    config_name = config_file.stem
                    configs[config_name] = config
            except Exception as e:
                print(f"Warning: Failed to load {config_file.name}: {e}")

        return configs

    def list_apis(self) -> List[str]:
        """
        Liste toutes les APIs disponibles.

        Returns:
            Liste des noms d'APIs uniques
        """
        apis = set()

        for config in self.configs.values():
            api_name = config.get("source", {}).get("name")
            if api_name:
                apis.add(api_name)

        return sorted(list(apis))

    def list_endpoints(self, api: str) -> List[str]:
        """
        Liste tous les endpoints disponibles pour une API.

        Args:
            api: Nom de l'API

        Returns:
            Liste des noms d'endpoints
        """
        endpoints = []

        for config_name, config in self.configs.items():
            if config.get("source", {}).get("name") == api:
                endpoint = config.get("resource", {}).get("name")
                if endpoint:
                    endpoints.append(endpoint)

        return sorted(endpoints)

    def get_config(self, api: str, endpoint: str) -> Dict:
        """
        Récupère la configuration pour un API/endpoint donné.

        Args:
            api: Nom de l'API
            endpoint: Nom de l'endpoint

        Returns:
            Configuration complète

        Raises:
            ValueError: Si la configuration n'existe pas
        """
        # Chercher par nom de resource
        for config_name, config in self.configs.items():
            resource_name = config.get("resource", {}).get("name", "")
            if resource_name == f"{api}_{endpoint}" or resource_name == endpoint:
                return config

        # Chercher par nom de fichier
        config_file = self.config_dir / f"{api}_{endpoint}.yml"
        if config_file.exists():
            return load_config(str(config_file))

        raise ValueError(f"Configuration not found for {api}/{endpoint}")

    def get_data(
        self,
        api: str,
        endpoint: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **params
    ) -> Iterator[Dict]:
        """
        Récupère des données d'un endpoint Hub'Eau.

        Args:
            api: Nom de l'API (ex: "hydrometry")
            endpoint: Nom de l'endpoint (ex: "stations")
            start_date: Date de début (YYYY-MM-DD)
            end_date: Date de fin (YYYY-MM-DD)
            **params: Paramètres additionnels

        Yields:
            Records de données

        Example:
            >>> client = HubeauClient()
            >>> for station in client.get_data("hydrometry", "stations"):
            ...     print(station["code_station"])
        """
        # Récupérer la configuration
        config = self.get_config(api, endpoint)

        # Sauvegarder la config dans un fichier temporaire
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            yaml.dump(config, f)
            temp_config_path = f.name

        try:
            # Créer la source DLT
            source = hubeau_source(
                config_path=temp_config_path,
                **params
            )

            # Exécuter la source et yield les données
            for resource in source:
                yield from resource
        finally:
            # Nettoyer le fichier temporaire
            os.unlink(temp_config_path)

    def save_to_postgres(
        self,
        data: Iterator[Dict],
        table: str,
        schema: str = "bronze",
        database: str = "hubeau"
    ):
        """
        Sauvegarde des données vers PostgreSQL.

        Args:
            data: Iterator de records
            table: Nom de la table
            schema: Nom du schéma (défaut: "bronze")
            database: Nom de la base de données

        Example:
            >>> client = HubeauClient()
            >>> data = client.get_data("hydrometry", "stations")
            >>> client.save_to_postgres(data, "hydrometry_stations")
        """
        import os

        # Configuration PostgreSQL
        credentials = {
            "database": os.getenv("POSTGRES_DB", database),
            "username": os.getenv("POSTGRES_USER", "hubeau"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", 5432))
        }

        # Créer un pipeline DLT temporaire
        pipeline = dlt.pipeline(
            pipeline_name=f"hubeau_client_{table}",
            destination=dlt.destinations.postgres(credentials=credentials),
            dataset_name=schema
        )

        # Charger les données
        info = pipeline.run(data, table_name=table)

        print(f"Loaded {sum(info.load_packages[0].jobs.values())} records to {schema}.{table}")

        return info

    def save_to_parquet(
        self,
        data: Iterator[Dict],
        path: str,
        partition_by: Optional[List[str]] = None
    ):
        """
        Sauvegarde des données vers fichiers Parquet.

        Args:
            data: Iterator de records
            path: Chemin du fichier ou répertoire
            partition_by: Colonnes pour partitionnement

        Example:
            >>> client = HubeauClient()
            >>> data = client.get_data("hydrometry", "stations")
            >>> client.save_to_parquet(data, "output/stations.parquet")
        """
        import pandas as pd
        from pathlib import Path

        # Convertir en liste pour pandas
        records = list(data)

        if not records:
            print("No data to save")
            return

        df = pd.DataFrame(records)

        # Créer le répertoire si nécessaire
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Sauvegarder
        if partition_by:
            # Partitionnement
            df.to_parquet(
                path,
                partition_cols=partition_by,
                index=False
            )
        else:
            # Fichier unique
            df.to_parquet(path, index=False)

        print(f"Saved {len(records)} records to {path}")

    def save_to_csv(
        self,
        data: Iterator[Dict],
        path: str,
        delimiter: str = ","
    ):
        """
        Sauvegarde des données vers CSV.

        Args:
            data: Iterator de records
            path: Chemin du fichier CSV
            delimiter: Délimiteur (défaut: ",")

        Example:
            >>> client = HubeauClient()
            >>> data = client.get_data("hydrometry", "stations")
            >>> client.save_to_csv(data, "output/stations.csv")
        """
        import pandas as pd
        from pathlib import Path

        # Convertir en DataFrame
        records = list(data)

        if not records:
            print("No data to save")
            return

        df = pd.DataFrame(records)

        # Créer le répertoire si nécessaire
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Sauvegarder
        df.to_csv(path, index=False, sep=delimiter)

        print(f"Saved {len(records)} records to {path}")

    def create_pipeline(self, destination: str = "postgres") -> 'HubeauPipeline':
        """
        Crée un pipeline Hub'Eau configuré.

        Args:
            destination: Type de destination (postgres, filesystem, duckdb)

        Returns:
            Instance de HubeauPipeline

        Example:
            >>> client = HubeauClient()
            >>> pipeline = client.create_pipeline("postgres")
            >>> pipeline.run("hydrometry", "stations")
        """
        from .pipeline import HubeauPipeline
        return HubeauPipeline(client=self, destination=destination)

    def get_stats(self) -> Dict[str, Any]:
        """
        Récupère des statistiques sur les configurations disponibles.

        Returns:
            Dict avec statistiques
        """
        apis = self.list_apis()

        stats = {
            "total_configs": len(self.configs),
            "apis": len(apis),
            "api_list": apis,
            "endpoints_by_api": {}
        }

        for api in apis:
            endpoints = self.list_endpoints(api)
            stats["endpoints_by_api"][api] = {
                "count": len(endpoints),
                "endpoints": endpoints
            }

        return stats
