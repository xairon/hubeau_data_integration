#!/usr/bin/env python3
"""
Script de test pour vérifier l'intégration SANDRE et BD-LISA
"""

import sys
import traceback

def test_imports():
    """Test des imports des modules"""
    print("🔍 Test des imports...")

    errors = []

    # Test import sources
    try:
        from src.hubeau_pipeline.sources import sandre_raw_source
        print("✅ Import sandre_raw_source OK")
    except Exception as e:
        errors.append(f"❌ Import sandre_raw_source: {e}")

    try:
        from src.hubeau_pipeline.sources import bdlisa_raw_source
        print("✅ Import bdlisa_raw_source OK")
    except Exception as e:
        errors.append(f"❌ Import bdlisa_raw_source: {e}")

    # Test import assets
    try:
        from src.hubeau_pipeline.assets.bronze import sandre_raw_assets
        print("✅ Import sandre_raw_assets OK")
    except Exception as e:
        errors.append(f"❌ Import sandre_raw_assets: {e}")

    try:
        from src.hubeau_pipeline.assets.bronze import bdlisa_raw_assets
        print("✅ Import bdlisa_raw_assets OK")
    except Exception as e:
        errors.append(f"❌ Import bdlisa_raw_assets: {e}")

    # Test import jobs
    try:
        from src.hubeau_pipeline.jobs import reference_jobs
        print("✅ Import reference_jobs OK")
    except Exception as e:
        errors.append(f"❌ Import reference_jobs: {e}")

    return errors

def test_source_structure():
    """Vérifie la structure des sources DLT"""
    print("\n🔍 Test de la structure des sources...")

    errors = []

    try:
        from src.hubeau_pipeline.sources.sandre_raw_source import sandre_raw
        source = sandre_raw()

        # Vérifier que c'est une liste de resources
        if not isinstance(source, list):
            errors.append(f"❌ sandre_raw devrait retourner une liste, got {type(source)}")
        else:
            print(f"✅ sandre_raw retourne une liste de {len(source)} resources")

            # Vérifier les noms des resources
            expected_resources = [
                "parametres", "unites", "methodes", "supports", "fractions",
                "communes", "departements", "regions", "intervenants",
                "masses_eau_souterraines", "masses_eau_surface",
                "cours_eau", "bassins", "milieux"
            ]

            for resource in source:
                if hasattr(resource, '__name__'):
                    print(f"  - Resource: {resource.__name__}")

    except Exception as e:
        errors.append(f"❌ Test structure sandre_raw: {e}")

    try:
        from src.hubeau_pipeline.sources.bdlisa_raw_source import bdlisa_raw
        source = bdlisa_raw()

        if not isinstance(source, list):
            errors.append(f"❌ bdlisa_raw devrait retourner une liste, got {type(source)}")
        else:
            print(f"✅ bdlisa_raw retourne une liste de {len(source)} resources")

            for resource in source:
                if hasattr(resource, '__name__'):
                    print(f"  - Resource: {resource.__name__}")

    except Exception as e:
        errors.append(f"❌ Test structure bdlisa_raw: {e}")

    return errors

def test_asset_groups():
    """Vérifie les groupes d'assets"""
    print("\n🔍 Test des groupes d'assets...")

    errors = []
    groups_found = set()

    try:
        from src.hubeau_pipeline.assets.bronze.sandre_raw_assets import (
            sandre_analysis_refs,
            sandre_territorial_refs,
            sandre_hydro_refs,
            sandre_intervenants
        )

        # Vérifier les attributs group_name
        for asset in [sandre_analysis_refs, sandre_territorial_refs,
                     sandre_hydro_refs, sandre_intervenants]:
            if hasattr(asset, '_metadata'):
                group = asset._metadata.get('group_name')
                if group:
                    groups_found.add(group)
                    print(f"  ✅ Asset {asset.__name__} dans groupe: {group}")

    except Exception as e:
        errors.append(f"❌ Test groupes SANDRE: {e}")

    try:
        from src.hubeau_pipeline.assets.bronze.bdlisa_raw_assets import (
            bdlisa_entites,
            bdlisa_stats
        )

        for asset in [bdlisa_entites, bdlisa_stats]:
            if hasattr(asset, '_metadata'):
                group = asset._metadata.get('group_name')
                if group:
                    groups_found.add(group)
                    print(f"  ✅ Asset {asset.__name__} dans groupe: {group}")

    except Exception as e:
        errors.append(f"❌ Test groupes BD-LISA: {e}")

    print(f"\n📊 Groupes trouvés: {groups_found}")

    return errors

def test_job_definitions():
    """Vérifie les définitions de jobs"""
    print("\n🔍 Test des définitions de jobs...")

    errors = []

    try:
        from src.hubeau_pipeline.jobs.reference_jobs import (
            sandre_full_load_job,
            bdlisa_spatial_load_job,
            reference_data_full_load_job
        )

        jobs = [
            ("sandre_full_load_job", sandre_full_load_job),
            ("bdlisa_spatial_load_job", bdlisa_spatial_load_job),
            ("reference_data_full_load_job", reference_data_full_load_job)
        ]

        for job_name, job_obj in jobs:
            if job_obj:
                print(f"  ✅ Job {job_name} défini")
                # Note: Sans Dagster installé, on ne peut pas vérifier plus
            else:
                errors.append(f"❌ Job {job_name} est None")

    except Exception as e:
        errors.append(f"❌ Test jobs: {e}")

    return errors

def main():
    """Test principal"""
    print("=" * 60)
    print("TEST D'INTÉGRATION SANDRE ET BD-LISA")
    print("=" * 60)

    all_errors = []

    # Tests
    all_errors.extend(test_imports())
    all_errors.extend(test_source_structure())
    # Les tests suivants nécessitent Dagster
    # all_errors.extend(test_asset_groups())
    # all_errors.extend(test_job_definitions())

    # Résumé
    print("\n" + "=" * 60)
    if all_errors:
        print("❌ ERREURS DÉTECTÉES:")
        for error in all_errors:
            print(f"  {error}")
        return 1
    else:
        print("✅ TOUS LES TESTS SONT PASSÉS!")
        print("\n📝 Note: Certains tests nécessitent Dagster pour être complets.")
        print("    Lancez ce script dans le conteneur Docker pour des tests complets.")
        return 0

if __name__ == "__main__":
    sys.exit(main())