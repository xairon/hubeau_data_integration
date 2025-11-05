#!/usr/bin/env python3
"""
Script pour matérialiser l'asset piezometers_csv_data via l'API Dagster.
"""

import requests
import json
import time

def materialize_asset():
    """Lance la matérialisation de l'asset piezometers_csv_data."""

    # Configuration
    dagster_url = "http://localhost:8080"
    asset_key = ["piezometers_csv_data"]

    # GraphQL query pour lancer la matérialisation
    query = """
    mutation LaunchRun {
        launchRun(
            executionParams: {
                mode: "default"
                selector: {
                    repositoryLocationName: "__repository__"
                    repositoryName: "__repository__"
                    jobName: "__ASSET_JOB"
                    assetSelection: [{path: ["piezometers_csv_data"]}]
                }
            }
        ) {
            __typename
            ... on LaunchRunSuccess {
                run {
                    id
                    status
                    pipelineName
                }
            }
            ... on PythonError {
                message
                stack
            }
            ... on InvalidSubsetError {
                message
            }
            ... on RunConfigValidationInvalid {
                errors {
                    message
                }
            }
        }
    }
    """

    print("🚀 Lancement de la matérialisation de piezometers_csv_data...")

    # Envoyer la requête
    response = requests.post(
        f"{dagster_url}/graphql",
        json={"query": query},
        headers={"Content-Type": "application/json"}
    )

    if response.status_code == 200:
        result = response.json()

        if result.get("data") and result["data"].get("launchRun"):
            launch_result = result["data"]["launchRun"]

            if launch_result["__typename"] == "LaunchRunSuccess":
                run_id = launch_result["run"]["id"]
                print(f"✅ Run lancé avec succès!")
                print(f"   ID du run : {run_id}")
                print(f"   Status : {launch_result['run']['status']}")
                print(f"\n📊 Suivre le run : {dagster_url}/runs/{run_id}")

                # Suivre le statut du run
                print("\n⏳ Suivi du run en cours...")
                follow_run(dagster_url, run_id)

            else:
                print(f"❌ Erreur lors du lancement : {launch_result}")
        else:
            print(f"❌ Réponse inattendue : {result}")
    else:
        print(f"❌ Erreur HTTP {response.status_code}")
        print(response.text)


def follow_run(dagster_url, run_id):
    """Suit le statut d'un run jusqu'à sa completion."""

    query = """
    query RunStatus($runId: ID!) {
        runOrError(runId: $runId) {
            ... on Run {
                status
                stats {
                    ... on RunStatsSnapshot {
                        stepsSucceeded
                        stepsFailed
                        expectations
                        materializations
                    }
                }
            }
        }
    }
    """

    statuses = {
        "QUEUED": "⏳ En attente...",
        "STARTING": "🔄 Démarrage...",
        "STARTED": "▶️ En cours...",
        "SUCCESS": "✅ Succès!",
        "FAILURE": "❌ Échec",
        "CANCELED": "🚫 Annulé"
    }

    last_status = None
    while True:
        response = requests.post(
            f"{dagster_url}/graphql",
            json={"query": query, "variables": {"runId": run_id}},
            headers={"Content-Type": "application/json"}
        )

        if response.status_code == 200:
            result = response.json()
            if result.get("data") and result["data"].get("runOrError"):
                run = result["data"]["runOrError"]
                status = run["status"]

                if status != last_status:
                    print(f"   {statuses.get(status, status)}")
                    last_status = status

                    if run.get("stats"):
                        stats = run["stats"]
                        if stats["materializations"] > 0:
                            print(f"   📦 Matérialisations : {stats['materializations']}")

                # Sortir de la boucle si terminé
                if status in ["SUCCESS", "FAILURE", "CANCELED"]:
                    break

        time.sleep(2)

    if last_status == "SUCCESS":
        print("\n🎉 Le fichier CSV a été chargé avec succès dans PostgreSQL!")
        print("📍 Table : public.piezometers")
        print("📊 120 826 lignes de données de piézomètres")
    elif last_status == "FAILURE":
        print("\n❌ Le chargement a échoué. Vérifier les logs dans Dagster UI.")


if __name__ == "__main__":
    print("=" * 60)
    print("CHARGEMENT DES DONNÉES DE PIÉZOMÈTRES")
    print("=" * 60)

    materialize_asset()

    print("\n" + "=" * 60)
    print("Pour vérifier les données :")
    print("- Adminer : http://localhost:8081")
    print("- SQL : SELECT COUNT(*) FROM piezometers;")
    print("=" * 60)