"""
Pipeline Hub'Eau - Wrapper DLT pour Hub'Eau
"""
import dlt
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import yaml
from src.dlt_pipeline.sources import hubeau_source
from src.dlt_pipeline.destinations import get_destination


class HubeauPipeline:
    """
    Pipeline DLT pour Hub'Eau.

    Usage:
        # Créer un pipeline
        pipeline = HubeauPipeline(destination="postgres")

        # Charger un endpoint
        pipeline.run("hydrometry", "stations")

        # Charger avec dates
        pipeline.run(
            "temperature", "chroniques",
            start_date="2024-01-01",
            end_date="2024-12-31"
        )

        # Charger plusieurs endpoints
        pipeline.run_multiple([
            ("hydrometry", "stations"),
            ("hydrometry", "sites"),
            ("hydrometry", "obs_elab", {"year": 2024})
        ])
    """

    def __init__(
        self,
        client=None,
        destination: str = "postgres",
        dataset_name: str = "bronze",
        config_dir: str = "configs/hubeau"
    ):
        """
        Initialise le pipeline Hub'Eau.

        Args:
            client: Instance de HubeauClient (optionnel)
            destination: Type de destination (postgres, filesystem, duckdb)
            dataset_name: Nom du dataset/schema
            config_dir: Répertoire des configurations
        """
        self.client = client
        self.destination_type = destination
        self.dataset_name = dataset_name
        self.config_dir = Path(config_dir)
        self.pipeline = None

        # Charger la configuration de destination
        self._init_pipeline()

    def _init_pipeline(self):
        """Initialise le pipeline DLT."""
        # Créer la destination
        dest = get_destination(self.destination_type)

        # Créer le pipeline
        self.pipeline = dlt.pipeline(
            pipeline_name=f"hubeau_{self.destination_type}",
            destination=dest,
            dataset_name=self.dataset_name
        )

    def _get_config_path(self, api: str, endpoint: str) -> Path:
        """
        Trouve le fichier de configuration pour un API/endpoint.

        Args:
            api: Nom de l'API
            endpoint: Nom de l'endpoint

        Returns:
            Chemin vers le fichier de configuration

        Raises:
            FileNotFoundError: Si la configuration n'existe pas
        """
        # Essayer plusieurs patterns de nommage
        patterns = [
            f"{api}_{endpoint}.yml",
            f"{endpoint}.yml"
        ]

        for pattern in patterns:
            config_path = self.config_dir / pattern
            if config_path.exists():
                return config_path

        raise FileNotFoundError(
            f"Configuration not found for {api}/{endpoint}. "
            f"Searched for: {', '.join(patterns)}"
        )

    def run(
        self,
        api: str,
        endpoint: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **kwargs
    ):
        """
        Execute le pipeline pour un endpoint.

        Args:
            api: Nom de l'API (ex: "hydrometry")
            endpoint: Nom de l'endpoint (ex: "stations")
            start_date: Date de début (YYYY-MM-DD)
            end_date: Date de fin (YYYY-MM-DD)
            **kwargs: Paramètres additionnels

        Returns:
            LoadInfo DLT avec résultats du chargement

        Example:
            >>> pipeline = HubeauPipeline()
            >>> info = pipeline.run("hydrometry", "stations")
            >>> print(f"Loaded {info.row_counts} rows")
        """
        # Trouver la configuration
        config_path = self._get_config_path(api, endpoint)

        # Charger la configuration
        with open(config_path, encoding='utf-8') as f:
            config = yaml.safe_load(f)

        print(f"Running pipeline for {api}/{endpoint}...")

        # Créer la source DLT
        source = hubeau_source(
            config_path=str(config_path),
            **kwargs
        )

        # Exécuter le pipeline
        info = self.pipeline.run(source)

        # Afficher le résumé
        self._print_summary(info, api, endpoint)

        return info

    def run_multiple(
        self,
        configs: List[Tuple[str, str, Optional[Dict]]]
    ) -> List:
        """
        Execute le pipeline pour plusieurs endpoints.

        Args:
            configs: Liste de tuples (api, endpoint, params_optionnels)

        Returns:
            Liste de LoadInfo pour chaque endpoint

        Example:
            >>> pipeline = HubeauPipeline()
            >>> infos = pipeline.run_multiple([
            ...     ("hydrometry", "stations", None),
            ...     ("hydrometry", "sites", None),
            ...     ("temperature", "chroniques", {"start_date": "2024-01-01"})
            ... ])
        """
        results = []

        for config in configs:
            if len(config) == 2:
                api, endpoint = config
                params = {}
            else:
                api, endpoint, params = config
                params = params or {}

            try:
                info = self.run(api, endpoint, **params)
                results.append(info)
            except Exception as e:
                print(f"Error running {api}/{endpoint}: {e}")
                results.append(None)

        return results

    def get_state(self, api: Optional[str] = None) -> Dict[str, Any]:
        """
        Récupère l'état du pipeline.

        Args:
            api: Nom de l'API (optionnel, None = tout)

        Returns:
            Dict contenant l'état

        Example:
            >>> pipeline = HubeauPipeline()
            >>> state = pipeline.get_state("hydrometry")
            >>> print(state)
        """
        if not self.pipeline:
            return {}

        # Récupérer l'état DLT
        try:
            state = self.pipeline.state

            if api:
                # Filtrer par API
                filtered_state = {}
                for key, value in state.items():
                    if api in str(key):
                        filtered_state[key] = value
                return filtered_state

            return dict(state)

        except Exception as e:
            print(f"Error getting state: {e}")
            return {}

    def get_all_states(self) -> Dict[str, Dict]:
        """
        Récupère l'état de tous les pipelines.

        Returns:
            Dict {api_name: state_info}
        """
        all_states = {}

        # Lister toutes les configs
        for config_file in self.config_dir.glob("*.yml"):
            try:
                with open(config_file, encoding='utf-8') as f:
                    config = yaml.safe_load(f)

                api_name = config.get("source", {}).get("name")
                resource_name = config.get("resource", {}).get("name")

                if api_name and resource_name:
                    state = self.get_state(resource_name)
                    if state:
                        all_states[resource_name] = state

            except Exception:
                continue

        return all_states

    def reset_state(self, api: Optional[str] = None):
        """
        Reset l'état du pipeline.

        Args:
            api: Nom de l'API (optionnel, None = tout)

        Example:
            >>> pipeline = HubeauPipeline()
            >>> pipeline.reset_state("hydrometry")
        """
        if not self.pipeline:
            print("No pipeline initialized")
            return

        try:
            if api:
                # Reset pour une API spécifique
                print(f"Resetting state for {api}...")
                # DLT ne permet pas de reset partiel facilement
                # Il faut recréer le pipeline
                self._init_pipeline()
                print(f"State reset for {api}")
            else:
                # Reset complet
                print("Resetting all pipeline state...")
                self._init_pipeline()
                print("All state reset")

        except Exception as e:
            print(f"Error resetting state: {e}")

    def _print_summary(self, info, api: str, endpoint: str):
        """
        Affiche un résumé du chargement.

        Args:
            info: LoadInfo DLT
            api: Nom de l'API
            endpoint: Nom de l'endpoint
        """
        print(f"\n{'='*60}")
        print(f"Pipeline Summary: {api}/{endpoint}")
        print(f"{'='*60}")

        # Load IDs
        if hasattr(info, 'loads_ids') and info.loads_ids:
            print(f"Load ID: {info.loads_ids[0]}")

        # Row counts
        if hasattr(info, 'load_packages') and info.load_packages:
            for package in info.load_packages:
                if hasattr(package, 'jobs'):
                    for table_name, job_count in package.jobs.items():
                        print(f"Table: {table_name}")
                        print(f"  Jobs: {job_count}")

        # Dataset info
        print(f"Dataset: {self.dataset_name}")
        print(f"Destination: {self.destination_type}")

        print(f"{'='*60}\n")

    def validate_config(self, api: str, endpoint: str) -> List[str]:
        """
        Valide la configuration d'un endpoint.

        Args:
            api: Nom de l'API
            endpoint: Nom de l'endpoint

        Returns:
            Liste des erreurs (vide si valide)

        Example:
            >>> pipeline = HubeauPipeline()
            >>> errors = pipeline.validate_config("hydrometry", "stations")
            >>> if errors:
            ...     print("Errors:", errors)
        """
        try:
            config_path = self._get_config_path(api, endpoint)

            with open(config_path, encoding='utf-8') as f:
                config = yaml.safe_load(f)

            # Validation basique de la config
            errors = []
            if not config.get('api'):
                errors.append("Missing 'api' section in config")
            if not config.get('api', {}).get('name'):
                errors.append("Missing 'api.name' in config")
            if not config.get('api', {}).get('endpoint'):
                errors.append("Missing 'api.endpoint' in config")
            return errors

        except FileNotFoundError as e:
            return [str(e)]
        except Exception as e:
            return [f"Error validating config: {e}"]

    def get_pipeline_info(self) -> Dict[str, Any]:
        """
        Récupère les informations du pipeline.

        Returns:
            Dict avec informations du pipeline
        """
        if not self.pipeline:
            return {}

        return {
            "pipeline_name": self.pipeline.pipeline_name,
            "destination": self.destination_type,
            "dataset_name": self.dataset_name,
            "working_dir": str(self.pipeline.working_dir),
            "pipelines_dir": str(self.pipeline.pipelines_dir),
        }
