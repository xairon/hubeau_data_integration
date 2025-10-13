"""
CLI Hub'Eau - Interface en ligne de commande
============================================

Usage:
    python -m hubeau.cli list-apis
    python -m hubeau.cli list-endpoints piezometry
    python -m hubeau.cli test-connectivity
    python -m hubeau.cli get-data piezometry stations --limit 10
    python -m hubeau.cli get-data piezometry stations --limit 100 --export csv --output ./data/
"""

import click
import json
import sys
from pathlib import Path
from typing import Optional

from .client import HubeauClient
from .pipeline import HubeauPipeline


@click.group()
@click.version_option(version="1.0.0")
def cli():
    """
    Hub'Eau CLI - Interface en ligne de commande pour les APIs Hub'Eau

    Exemples:
        hubeau list-apis
        hubeau get-data piezometry stations --limit 10
    """
    pass


@cli.command("list-apis")
def list_apis():
    """Liste toutes les APIs Hub'Eau disponibles"""
    client = HubeauClient()
    apis = client.list_apis()

    click.echo("\nAPIs Hub'Eau disponibles:\n")
    for i, api in enumerate(apis, 1):
        click.echo(f"  {i}. {api}")
    click.echo(f"\nTotal: {len(apis)} APIs\n")


@cli.command("list-endpoints")
@click.argument("api")
def list_endpoints(api: str):
    """
    Liste tous les endpoints d'une API spécifique

    Exemples:
        hubeau list-endpoints piezometry
        hubeau list-endpoints quality_rivers
    """
    client = HubeauClient()

    try:
        endpoints = client.list_endpoints(api)

        click.echo(f"\nEndpoints pour '{api}':\n")
        for endpoint in endpoints:
            click.echo(f"  - {endpoint}")
        click.echo(f"\nTotal: {len(endpoints)} endpoints\n")

    except ValueError as e:
        click.echo(f"Erreur: {e}", err=True)
        sys.exit(1)


@cli.command("test-connectivity")
@click.option("--api", help="Tester une API spécifique")
def test_connectivity(api: Optional[str] = None):
    """
    Teste la connectivité aux APIs Hub'Eau

    Exemples:
        hubeau test-connectivity
        hubeau test-connectivity --api piezometry
    """
    client = HubeauClient()

    click.echo("\n" + "=" * 80)
    click.echo("TEST DE CONNECTIVITE HUB'EAU")
    click.echo("=" * 80 + "\n")

    if api:
        # Test une API spécifique
        apis = [api]
    else:
        # Test toutes les APIs
        apis = client.list_apis()

    results = []

    for api_name in apis:
        endpoints = client.list_endpoints(api_name)

        for endpoint in endpoints:
            try:
                # Tenter d'extraire 1 record
                data = list(client.get_data(api_name, endpoint, limit=1))
                status = "[OK]"
                message = f"{len(data)} record(s)"
            except Exception as e:
                status = "[FAIL]"
                message = str(e)[:50]

            results.append({
                "api": api_name,
                "endpoint": endpoint,
                "status": status,
                "message": message
            })

            # Afficher résultat
            full_name = f"{api_name}/{endpoint}".ljust(40)
            click.echo(f"{status} {full_name} - {message}")

    # Statistiques
    success = sum(1 for r in results if r["status"] == "[OK]")
    total = len(results)
    percentage = (success / total * 100) if total > 0 else 0

    click.echo("\n" + "=" * 80)
    click.echo(f"Taux de succès: {percentage:.1f}% ({success}/{total})")
    click.echo("=" * 80 + "\n")


@cli.command("get-data")
@click.argument("api")
@click.argument("endpoint")
@click.option("--limit", default=10, help="Nombre maximum de records (défaut: 10)")
@click.option("--params", default=None, help="Paramètres JSON (ex: '{\"code_departement\": \"75\"}')")
@click.option("--export", type=click.Choice(["csv", "json", "parquet"]), help="Format d'export")
@click.option("--output", default="./data/", help="Répertoire de sortie (défaut: ./data/)")
def get_data(
    api: str,
    endpoint: str,
    limit: int,
    params: Optional[str],
    export: Optional[str],
    output: str
):
    """
    Extrait des données d'une API Hub'Eau

    Exemples:
        hubeau get-data piezometry stations --limit 10
        hubeau get-data ecoulement stations --params '{"code_departement": "75"}' --limit 5
        hubeau get-data piezometry stations --limit 100 --export csv --output ./data/
    """
    client = HubeauClient()

    # Parser les paramètres JSON si fournis
    query_params = {}
    if params:
        try:
            query_params = json.loads(params)
        except json.JSONDecodeError:
            click.echo("Erreur: Paramètres JSON invalides", err=True)
            sys.exit(1)

    # Extraire les données
    click.echo(f"\nExtraction de {api}/{endpoint}...")

    try:
        data = list(client.get_data(
            api=api,
            endpoint=endpoint,
            params=query_params,
            limit=limit
        ))

        click.echo(f"Extraits: {len(data)} records\n")

        # Export si demandé
        if export:
            import pandas as pd

            output_dir = Path(output)
            output_dir.mkdir(parents=True, exist_ok=True)

            df = pd.DataFrame(data)

            if export == "csv":
                filepath = output_dir / f"{api}_{endpoint}.csv"
                df.to_csv(filepath, index=False, encoding='utf-8-sig')
            elif export == "json":
                filepath = output_dir / f"{api}_{endpoint}.json"
                df.to_json(filepath, orient="records", indent=2)
            elif export == "parquet":
                filepath = output_dir / f"{api}_{endpoint}.parquet"
                df.to_parquet(filepath, index=False)

            click.echo(f"Exporté vers: {filepath}\n")
        else:
            # Afficher les données en JSON
            click.echo(json.dumps(data, indent=2, ensure_ascii=False))

    except Exception as e:
        click.echo(f"Erreur: {e}", err=True)
        sys.exit(1)


@cli.command("run-pipeline")
@click.argument("api")
@click.argument("endpoint")
@click.option("--destination", default="duckdb", type=click.Choice(["duckdb", "postgres", "filesystem"]))
@click.option("--dataset", default="hubeau", help="Nom du dataset (défaut: hubeau)")
def run_pipeline(api: str, endpoint: str, destination: str, dataset: str):
    """
    Exécute un pipeline DLT complet

    Exemples:
        hubeau run-pipeline piezometry stations
        hubeau run-pipeline piezometry stations --destination postgres --dataset test
    """
    click.echo(f"\nExécution pipeline {destination}...")
    click.echo(f"API: {api}")
    click.echo(f"Endpoint: {endpoint}")
    click.echo(f"Dataset: {dataset}\n")

    try:
        pipeline = HubeauPipeline(
            destination=destination,
            dataset_name=dataset
        )

        result = pipeline.load(
            api=api,
            endpoint=endpoint
        )

        click.echo(f"\n✓ Pipeline exécuté avec succès")
        click.echo(f"Résultat: {result}\n")

    except Exception as e:
        click.echo(f"Erreur: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()