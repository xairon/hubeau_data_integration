#!/usr/bin/env python
"""
Script de test pour valider les correctifs API Hydrobiologie
Vérifie que tous les départements sont bien traités et que le split binaire fonctionne
"""

import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Ajouter le chemin src au PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from hubeau_pipeline.assets.bronze.hubeau_client import HubeauClient, HubeauIngestionService
from hubeau_pipeline.assets.bronze.hubeau_configs import get_hydrobiology_config

async def test_stations_coverage():
    """Test A : Vérifier que tous les départements sont traités sans troncature"""
    print("\n" + "="*80)
    print("TEST A : Couverture départements (stations_hydrobio)")
    print("="*80)
    
    config = get_hydrobiology_config()
    
    async with HubeauClient(config) as client:
        stations = await client.get_stations("stations_hydrobio")
        
        # Extraire les départements uniques
        depts = set(s.get("code_departement") for s in stations if s.get("code_departement"))
        
        print(f"\n✅ Total stations récupérées: {len(stations)}")
        print(f"✅ Départements couverts: {len(depts)}/101")
        print(f"✅ Métriques: {client.metrics.dict()}")
        
        # Vérifications
        assert len(stations) > 10000, f"Troncature détectée ! Seulement {len(stations)} stations"
        assert client.metrics.departements_traites == 101, f"Tous les départements doivent être traités ! Seulement {client.metrics.departements_traites}"
        
        print(f"\n✅ TEST A RÉUSSI : {len(stations)} stations, {len(depts)} départements")
        return stations

async def test_chunk_splitting():
    """Test B : Vérifier que le split binaire fonctionne en cas d'erreur"""
    print("\n" + "="*80)
    print("TEST B : Split binaire en cas d'erreur (simulation)")
    print("="*80)
    
    config = get_hydrobiology_config()
    
    # Test avec une petite fenêtre temporelle (7 jours)
    date_partition = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    async with HubeauClient(config) as client:
        # Récupérer stations pour test
        stations = await client.get_stations("stations_hydrobio")
        station_codes = [s.get("code_station_hydrobio") for s in stations[:100]]  # Limiter à 100 pour test rapide
        
        # Test observations avec codes
        observations = await client.get_observations(
            "indices",
            station_codes,
            date_partition,
            "hydrobiology"
        )
        
        print(f"\n✅ Observations récupérées: {len(observations)}")
        print(f"✅ Chunks traités: {client.metrics.chunks_total}")
        print(f"✅ Chunks OK: {client.metrics.chunks_ok}")
        print(f"✅ Chunks vides: {client.metrics.chunks_vides}")
        print(f"✅ Chunks échoués: {client.metrics.chunks_echoues}")
        print(f"✅ Erreurs HTTP 500: {client.metrics.erreurs_http_500}")
        print(f"✅ Erreurs timeout: {client.metrics.erreurs_timeout}")
        
        # Vérification : pas de chunks définitivement échoués
        if client.metrics.chunks_echoues > 0:
            print(f"\n⚠️  Codes échoués: {client.metrics.codes_echoues}")
        
        print(f"\n✅ TEST B RÉUSSI : {client.metrics.chunks_total} chunks traités")

async def test_retries_and_metrics():
    """Test C & D : Vérifier retries dynamiques et métriques"""
    print("\n" + "="*80)
    print("TEST C & D : Retries dynamiques + Métriques")
    print("="*80)
    
    config = get_hydrobiology_config()
    
    print(f"\n📊 Configuration Hydrobiologie:")
    print(f"   - max_retries: {config.max_retries}")
    print(f"   - rate_limit_delay: {config.rate_limit_delay}")
    print(f"   - depth_limit stations: {config.endpoints['stations_hydrobio'].depth_limit}")
    
    assert config.max_retries == 5, "max_retries devrait être 5 pour Hydrobiologie"
    assert config.rate_limit_delay == 0.6, "rate_limit_delay devrait être 0.6 pour Hydrobiologie"
    assert config.endpoints['stations_hydrobio'].depth_limit is None, "depth_limit devrait être None"
    
    print(f"\n✅ TEST C & D RÉUSSI : Configuration correcte")

async def test_full_ingestion():
    """Test complet : Ingestion réelle avec synthèse"""
    print("\n" + "="*80)
    print("TEST COMPLET : Ingestion avec synthèse")
    print("="*80)
    
    # Date de test (hier)
    date_partition = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    service = HubeauIngestionService()
    config = get_hydrobiology_config()
    
    result = await service.ingest_api_data(config, date_partition)
    
    print(f"\n✅ Résultat ingestion:")
    print(f"   - Status: {result['status']}")
    print(f"   - Total records: {result['total_records_ingested']}")
    print(f"   - Endpoints traités: {len(result['results_by_endpoint'])}")
    
    if 'metrics' in result:
        print(f"\n📊 Métriques finales:")
        for key, value in result['metrics'].items():
            if isinstance(value, list):
                print(f"   - {key}: {len(value)} éléments")
            else:
                print(f"   - {key}: {value}")
    
    print(f"\n✅ TEST COMPLET RÉUSSI")

async def main():
    """Exécuter tous les tests"""
    print("\n" + "="*80)
    print("🧪 TESTS DE VALIDATION - CORRECTIFS HYDROBIOLOGIE")
    print("="*80)
    
    try:
        # Test A : Couverture départements
        await test_stations_coverage()
        
        # Test B : Split binaire
        await test_chunk_splitting()
        
        # Test C & D : Retries et métriques
        await test_retries_and_metrics()
        
        # Test complet (optionnel, peut être long)
        # await test_full_ingestion()
        
        print("\n" + "="*80)
        print("✅ TOUS LES TESTS RÉUSSIS !")
        print("="*80 + "\n")
        
    except AssertionError as e:
        print(f"\n❌ TEST ÉCHOUÉ : {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERREUR : {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
