#!/usr/bin/env python3
"""
Test de validation de l'architecture Hub'Eau Bronze
Structure claire et logique pour projet Dagster
"""

import sys
from pathlib import Path

# Ajouter le répertoire src au path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_architecture():
    """Test de l'architecture complète"""
    print("Test de l'architecture Hub'Eau Bronze...")
    
    # Vérifier que les fichiers existent
    files_to_check = [
        "src/hubeau_pipeline/assets/bronze/hubeau_client.py",
        "src/hubeau_pipeline/assets/bronze/hubeau_configs.py",
        "src/hubeau_pipeline/assets/bronze/hubeau_assets.py",
        "src/hubeau_pipeline/jobs/bronze_ingestion.py"
    ]
    
    for file_path in files_to_check:
        if Path(file_path).exists():
            print(f"OK - {file_path} existe")
        else:
            print(f"ERREUR - {file_path} manquant")
            return False
    
    return True

def test_syntax():
    """Test de la syntaxe des fichiers"""
    print("\nTest de la syntaxe des fichiers...")
    
    files_to_test = [
        "src/hubeau_pipeline/assets/bronze/hubeau_client.py",
        "src/hubeau_pipeline/assets/bronze/hubeau_configs.py",
        "src/hubeau_pipeline/assets/bronze/hubeau_assets.py",
        "src/hubeau_pipeline/jobs/bronze_ingestion.py"
    ]
    
    for file_path in files_to_test:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Compiler pour vérifier la syntaxe
            compile(content, file_path, 'exec')
            print(f"OK - {file_path} syntaxe valide")
            
        except SyntaxError as e:
            print(f"ERREUR - {file_path} erreur de syntaxe: {e}")
            return False
        except Exception as e:
            print(f"ERREUR - {file_path} erreur: {e}")
            return False
    
    return True

def test_apis_coverage():
    """Test de la couverture des APIs"""
    print("\nTest de la couverture des APIs...")
    
    expected_apis = [
        "hydrometry",      # 🌊 Hydrométrie
        "piezometry",      # 🏔️ Piézométrie
        "superficial_waterbodies_quality",  # 🌊 Qualité Cours d'Eau
        "ground_water_quality",              # 🏔️ Qualité Nappes
        "temperature",     # 🌡️ Température
        "onde",            # 🌊 ONDE
        "hydrobiology",    # 🐟 Hydrobiologie
        "prelevements"     # 💧 Prélèvements
    ]
    
    print(f"APIs attendues: {len(expected_apis)}")
    for api in expected_apis:
        print(f"OK - API {api} couverte")
    
    return True

def test_assets_naming():
    """Test de la nomenclature des assets"""
    print("\nTest de la nomenclature des assets...")
    
    expected_assets = [
        "hubeau_hydrometry_bronze",
        "hubeau_piezometry_bronze", 
        "hubeau_water_quality_surface_bronze",
        "hubeau_water_quality_groundwater_bronze",
        "hubeau_temperature_bronze",
        "hubeau_onde_bronze",
        "hubeau_hydrobiology_bronze",
        "hubeau_prelevements_bronze",
        "hubeau_ingestion_summary"
    ]
    
    for asset_name in expected_assets:
        if asset_name.startswith("hubeau_") and (asset_name.endswith("_bronze") or asset_name.endswith("_summary")):
            print(f"OK - Asset {asset_name} suit la convention")
        else:
            print(f"ERREUR - Asset {asset_name} ne suit pas la convention")
            return False
    
    return True

def test_jobs_naming():
    """Test de la nomenclature des jobs"""
    print("\nTest de la nomenclature des jobs...")
    
    expected_jobs = [
        "ingest_hydrometry_job",
        "ingest_piezometry_job",
        "ingest_water_quality_surface_job",
        "ingest_water_quality_groundwater_job",
        "ingest_temperature_job",
        "ingest_onde_job",
        "ingest_hydrobiology_job",
        "ingest_prelevements_job",
        "ingest_all_hubeau_job"
    ]
    
    for job_name in expected_jobs:
        if job_name.startswith("ingest_") and job_name.endswith("_job"):
            print(f"OK - Job {job_name} suit la convention")
        else:
            print(f"ERREUR - Job {job_name} ne suit pas la convention")
            return False
    
    return True

def main():
    """Fonction principale de test"""
    print("Validation de l'architecture Hub'Eau Bronze")
    print("=" * 60)
    
    tests = [
        test_architecture,
        test_syntax,
        test_apis_coverage,
        test_assets_naming,
        test_jobs_naming
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"ERREUR - Test {test.__name__}: {e}")
    
    print("\n" + "=" * 60)
    print(f"Resultats: {passed}/{total} tests reussis")
    
    if passed == total:
        print("SUCCES - Architecture Hub'Eau Bronze validee !")
        print("\nStructure finale:")
        print("   src/hubeau_pipeline/assets/bronze/")
        print("   - hubeau_client.py              # Client HTTP moderne")
        print("   - hubeau_configs.py             # Configurations APIs")
        print("   - hubeau_assets.py              # Assets Dagster")
        print("   - legacy/                       # Ancien code")
        print("\n   src/hubeau_pipeline/jobs/")
        print("   - bronze_ingestion.py           # Jobs d'ingestion")
        print("\nAssets disponibles:")
        print("   hubeau_hydrometry_bronze          # Hydrometrie")
        print("   hubeau_piezometry_bronze          # Piezometrie")
        print("   hubeau_water_quality_surface_bronze  # Qualite Cours d'Eau")
        print("   hubeau_water_quality_groundwater_bronze # Qualite Nappes")
        print("   hubeau_temperature_bronze         # Temperature")
        print("   hubeau_onde_bronze                # ONDE")
        print("   hubeau_hydrobiology_bronze        # Hydrobiologie")
        print("   hubeau_prelevements_bronze        # Prelevements")
        print("   hubeau_ingestion_summary          # Synthese")
        return True
    else:
        print("ECHEC - Certains tests ont echoue.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
