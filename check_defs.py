#!/usr/bin/env python3
"""
Script de vérification des définitions Dagster
"""

import sys
import os

# Ajouter le répertoire src au PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    from hubeau_pipeline.definitions import defs
    print("✅ Définitions Dagster chargées avec succès")
    print(f"📊 Assets: {len(defs.assets)}")
    print(f"🔧 Jobs: {len(defs.jobs)}")
    print(f"⏰ Schedules: {len(defs.schedules)}")
    print(f"🔗 Resources: {len(defs.resources)}")
    
    # Lister les assets
    print("\n📋 Assets disponibles:")
    for asset in defs.assets:
        print(f"  - {asset.key}")
    
    # Lister les jobs
    print("\n🔧 Jobs disponibles:")
    for job in defs.jobs:
        print(f"  - {job.name}")
        
    print("\n🎉 Pipeline Hub'Eau prêt !")
    
except Exception as e:
    print(f"❌ Erreur lors du chargement des définitions: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
