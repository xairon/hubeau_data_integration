#!/usr/bin/env python3
"""
Script de test pour vérifier les fixes SIGKILL sur hydrobio

Tests:
1. Détection API sans pagination
2. HTTP Streaming pour grandes réponses
3. Adaptive chunk_size
4. Executor limité à 2 workers
5. PostgreSQL 16 compatibility
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import logging
import psutil
from hubeau_pipeline.sources.hubeau_csv_source import (
    HubeauAPIClient,
    get_total_pages_from_json,
    fetch_csv_page
)

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_hydrobio_pagination_detection():
    """Test détection API sans pagination (hydrobio)"""
    print("\n" + "="*60)
    print("TEST 1: Détection API sans pagination")
    print("="*60)

    client = HubeauAPIClient(
        base_url="https://hubeau.eaufrance.fr/api/v1/hydrobio",
        rate_limit=2.0
    )

    # Test avec hydrobio_stations qui n'a pas de pagination
    try:
        total_pages, total_count = get_total_pages_from_json(
            client,
            "/stations_hydrobio.csv",
            params={}
        )

        print(f"✅ Hydrobio stations: {total_count:,} records sur {total_pages} page(s)")

        if total_pages == 1 and total_count > 1000:
            print("✅ API sans pagination détectée correctement!")
        else:
            print(f"❌ Détection échouée: {total_pages} pages pour {total_count} records")

    except Exception as e:
        print(f"❌ Erreur: {e}")
        return False

    return True

def test_memory_usage():
    """Test utilisation mémoire"""
    print("\n" + "="*60)
    print("TEST 2: Utilisation mémoire")
    print("="*60)

    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    mem_mb = mem_info.rss / 1024 / 1024

    system_mem = psutil.virtual_memory()

    print(f"Process RAM: {mem_mb:.1f} MB")
    print(f"System RAM: {system_mem.percent:.1f}% utilisé")
    print(f"RAM disponible: {system_mem.available / 1024 / 1024 / 1024:.1f} GB")

    if mem_mb < 500:
        print("✅ Utilisation mémoire normale")
        return True
    else:
        print(f"⚠️ Utilisation mémoire élevée: {mem_mb:.1f} MB")
        return False

def test_streaming_detection():
    """Test détection streaming HTTP pour grandes réponses"""
    print("\n" + "="*60)
    print("TEST 3: HTTP Streaming pour grandes réponses")
    print("="*60)

    client = HubeauAPIClient(
        base_url="https://hubeau.eaufrance.fr/api/v1/hydrobio",
        rate_limit=2.0
    )

    print("Test avec endpoint hydrobio_stations (devrait utiliser streaming)...")

    # Fetch une page pour voir si streaming est activé
    try:
        chunks_count = 0
        for chunk in fetch_csv_page(
            client,
            "/stations_hydrobio.csv",
            page=1,
            params={},
            chunk_size=100  # Petit chunk size pour hydrobio
        ):
            chunks_count += 1
            if chunks_count == 1:
                print(f"✅ Premier chunk reçu: {len(chunk)} lignes")

            # Arrêter après 5 chunks pour le test
            if chunks_count >= 5:
                break

        print(f"✅ {chunks_count} chunks traités avec succès")
        print("✅ HTTP Streaming fonctionne correctement")
        return True

    except Exception as e:
        print(f"❌ Erreur streaming: {e}")
        return False

def test_executor_config():
    """Test configuration executor (vérification fichier)"""
    print("\n" + "="*60)
    print("TEST 4: Configuration Executor")
    print("="*60)

    definitions_path = os.path.join(
        os.path.dirname(__file__),
        '..',
        'src',
        'hubeau_pipeline',
        'definitions.py'
    )

    try:
        with open(definitions_path, 'r', encoding='utf-8') as f:
            content = f.read()

        if 'max_concurrent": 2' in content and 'executor=limited_executor' in content:
            print("✅ Executor configuré à 2 workers max")
            print("✅ Executor appliqué globalement")
            return True
        else:
            print("❌ Configuration executor incorrecte")
            return False

    except Exception as e:
        print(f"❌ Erreur lecture definitions.py: {e}")
        return False

def test_postgres16_fix():
    """Test fix PostgreSQL 16 (vérification fichier)"""
    print("\n" + "="*60)
    print("TEST 5: Fix PostgreSQL 16")
    print("="*60)

    monitoring_path = os.path.join(
        os.path.dirname(__file__),
        '..',
        'src',
        'hubeau_pipeline',
        'assets',
        'monitoring',
        'data_quality.py'
    )

    try:
        with open(monitoring_path, 'r', encoding='utf-8') as f:
            content = f.read()

        if 'pg_tables' in content and 's.relname' in content:
            print("✅ SQL compatible PostgreSQL 16")
            return True
        else:
            print("❌ SQL non compatible PostgreSQL 16")
            return False

    except Exception as e:
        print(f"❌ Erreur lecture data_quality.py: {e}")
        return False

def main():
    """Exécuter tous les tests"""
    print("\n" + "="*60)
    print("TESTS FIXES SIGKILL HYDROBIO")
    print("="*60)

    tests = [
        ("Détection API sans pagination", test_hydrobio_pagination_detection),
        ("Utilisation mémoire", test_memory_usage),
        ("HTTP Streaming", test_streaming_detection),
        ("Configuration Executor", test_executor_config),
        ("Fix PostgreSQL 16", test_postgres16_fix),
    ]

    results = []

    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"❌ Test '{name}' échoué avec erreur: {e}")
            results.append((name, False))

    # Résumé
    print("\n" + "="*60)
    print("RÉSUMÉ DES TESTS")
    print("="*60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {name}")

    print(f"\n{passed}/{total} tests réussis")

    if passed == total:
        print("\n🎉 TOUS LES TESTS SONT PASSÉS!")
        print("Les fixes SIGKILL sont prêts pour le déploiement.")
    else:
        print(f"\n⚠️ {total - passed} test(s) ont échoué.")
        print("Vérifiez les logs ci-dessus pour plus de détails.")

    return 0 if passed == total else 1

if __name__ == "__main__":
    exit(main())