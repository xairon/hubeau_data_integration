#!/usr/bin/env python3
"""
Test Local Simple - Hub'Eau Wrapper
====================================

Script minimal pour tester le wrapper Hub'Eau sans Docker.

Usage:
    python test_local_simple.py

Exports:
    - CSV dans data/local_tests/
    - Résumé dans le terminal
"""

import sys
from pathlib import Path

# Ajouter src/ au path pour import local
sys.path.insert(0, str(Path(__file__).parent / "src"))

from hubeau import HubeauClient
import pandas as pd


def main():
    """Test 3 APIs Hub'Eau et export CSV"""

    print("=" * 80)
    print("TEST LOCAL HUB'EAU - Sans Docker")
    print("=" * 80)
    print()

    # Créer répertoire de sortie
    output_dir = Path("data/local_tests")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Initialiser client
    client = HubeauClient()

    # Tests à exécuter
    tests = [
        {
            "name": "Piezometry stations",
            "api": "piezometry",
            "endpoint": "stations",
            "params": {},
            "limit": 10,
            "output": "piezometry_stations.csv"
        },
        {
            "name": "Temperature stations",
            "api": "temperature",
            "endpoint": "stations",
            "params": {},
            "limit": 10,
            "output": "temperature_stations.csv"
        },
        {
            "name": "Hydrometry stations",
            "api": "hydrometry",
            "endpoint": "stations",
            "params": {"code_departement": "69"},  # Rhône
            "limit": 10,
            "output": "hydrometry_stations.csv"
        }
    ]

    results = []

    # Exécuter les tests
    for test in tests:
        try:
            # Extraire données
            data = list(client.get_data(
                api=test["api"],
                endpoint=test["endpoint"],
                params=test["params"],
                limit=test["limit"]
            ))

            # Convertir en DataFrame
            df = pd.DataFrame(data)

            # Exporter CSV
            csv_path = output_dir / test["output"]
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')

            # Stocker résultat
            results.append({
                "status": "[OK]",
                "name": test["name"],
                "count": len(df),
                "file": test["output"]
            })

        except Exception as e:
            results.append({
                "status": "[FAIL]",
                "name": test["name"],
                "count": 0,
                "file": f"Error: {str(e)}"
            })

    # Afficher résultats
    print()
    for result in results:
        status_width = 6
        name_width = 25
        count_width = 15

        status = result["status"].ljust(status_width)
        name = result["name"].ljust(name_width)

        if result["status"] == "[OK]":
            count = f": {result['count']} records"
            file = f"-> {result['file']}"
            print(f"{status} {name} {count.ljust(count_width)} {file}")
        else:
            print(f"{status} {name} : {result['file']}")

    print()
    print("=" * 80)
    print(f"Tous les fichiers exportes dans: {output_dir.absolute()}")
    print("=" * 80)
    print()

    # Statistiques
    success = sum(1 for r in results if r["status"] == "[OK]")
    total = len(results)

    if success == total:
        print(f"Tous les tests reussis ({success}/{total})")
        print()
        print("Prochaines etapes:")
        print("  1. Consulter les CSV dans data/local_tests/")
        print("  2. Tester le notebook: jupyter notebook notebooks/test_hubeau_wrapper.ipynb")
        print("  3. Tester le CLI: python -m hubeau.cli list-apis")
        return 0
    else:
        print(f"Certains tests ont echoue ({success}/{total})")
        print()
        print("Verifier:")
        print("  - Connexion internet")
        print("  - Installation: pip install -r requirements.txt")
        return 1


if __name__ == "__main__":
    sys.exit(main())
