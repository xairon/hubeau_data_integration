"""
Package Hub'Eau - Wrapper autonome pour les APIs Hub'Eau

Ce package permet d'utiliser les APIs Hub'Eau sans Dagster,
directement en Python ou via CLI.

Usage:
    # Import du client
    from hubeau import HubeauClient, HubeauPipeline

    # Utilisation simple
    client = HubeauClient()
    data = client.get_data("hydrometry", "stations")

    # Pipeline complet
    pipeline = HubeauPipeline(destination="postgres")
    pipeline.run("hydrometry", "stations")
"""
from .client import HubeauClient
from .pipeline import HubeauPipeline

__all__ = ["HubeauClient", "HubeauPipeline"]
__version__ = "1.0.0"
