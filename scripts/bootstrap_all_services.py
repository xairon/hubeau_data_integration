#!/usr/bin/env python3
"""
Bootstrap complet de tous les services Hub'Eau Pipeline
MinIO + TimescaleDB + PostGIS + Neo4j
"""

import subprocess
import sys
import time
import os
from pathlib import Path

def run_script(script_path, description):
    """Exécuter un script d'initialisation"""
    print(f"\n🚀 {description}")
    print(f"📄 Script: {script_path}")
    
    try:
        result = subprocess.run([
            sys.executable, script_path
        ], capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            print(f"✅ {description} - SUCCÈS")
            if result.stdout:
                print(f"📝 Output:\n{result.stdout}")
            return True
        else:
            print(f"❌ {description} - ÉCHEC")
            if result.stderr:
                print(f"🚨 Erreur:\n{result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"⏱️ {description} - TIMEOUT (5min)")
        return False
    except Exception as e:
        print(f"❌ {description} - EXCEPTION: {e}")
        return False

def bootstrap_all():
    """Bootstrap complet de l'infrastructure"""
    print("🎯 === BOOTSTRAP HUB'EAU PIPELINE COMPLET ===")
    
    scripts_dir = Path(__file__).parent
    
    # Scripts d'initialisation dans l'ordre
    init_scripts = [
        {
            'script': scripts_dir / 'init_minio.py',
            'description': 'Initialisation MinIO (Buckets + Politiques)',
            'required': True
        },
        {
            'script': scripts_dir / 'bootstrap_minio.py',
            'description': 'Configuration MinIO avancée', 
            'required': False  # Si existe
        }
    ]
    
    success_count = 0
    total_required = sum(1 for s in init_scripts if s['required'])
    
    for script_config in init_scripts:
        script_path = script_config['script']
        description = script_config['description']
        required = script_config['required']
        
        if not script_path.exists():
            if required:
                print(f"❌ Script requis manquant: {script_path}")
                continue
            else:
                print(f"⚠️ Script optionnel absent: {script_path}")
                continue
        
        if run_script(script_path, description):
            success_count += 1
        elif required:
            print(f"🚨 Script requis échoué: {script_path}")
    
    # Résumé final
    print(f"\n📊 === RÉSUMÉ BOOTSTRAP ===")
    print(f"✅ Scripts réussis: {success_count}")
    print(f"📋 Scripts requis: {total_required}")
    
    if success_count >= total_required:
        print(f"🎉 BOOTSTRAP COMPLET - Infrastructure prête !")
        return True
    else:
        print(f"❌ BOOTSTRAP INCOMPLET - Vérifier les erreurs")
        return False

if __name__ == "__main__":
    success = bootstrap_all()
    sys.exit(0 if success else 1)
