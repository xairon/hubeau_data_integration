#!/usr/bin/env python3
"""
Script de test pour la nouvelle architecture dlt
Teste l'ingestion d'un endpoint Hub'Eau avec dlt
"""

import os
import sys
from pathlib import Path

# Ajouter le répertoire pipelines au PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent / "pipelines"))

from pipelines.dlt.hubeau_generic import run_pipeline
import yaml

def test_hydrobio_taxons():
    """Test l'ingestion des taxons hydrobiologiques avec dlt"""
    
    # Charger la configuration
    config_path = Path("configs/hubeau/hydrobio_taxons.yml")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    print("🔧 Configuration chargée:")
    print(f"  - Nom: {config['name']}")
    print(f"  - URL: {config['base_url']}{config['path']}")
    print(f"  - Clé de réplication: {config['replication_key']}")
    print(f"  - Fenêtre: {config['slicer']['window_days']} jour(s)")
    
    # Configuration MinIO (simulée pour le test)
    credentials = {
        "aws_access_key_id": os.getenv("MINIO_USER", "admin"),
        "aws_secret_access_key": os.getenv("MINIO_PASS", "password"),
        "endpoint_url": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        "region_name": "us-east-1",
    }
    
    print("\n🚀 Lancement du pipeline dlt...")
    
    try:
        # Lancer le pipeline
        result = run_pipeline(
            config=config,
            credentials=credentials,
            dataset_name="test_hydrobio",
            destination="filesystem"
        )
        
        print("✅ Pipeline terminé avec succès!")
        print(f"📊 Résultat: {result}")
        
    except Exception as e:
        print(f"❌ Erreur lors de l'exécution: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("🧪 Test de la nouvelle architecture dlt")
    print("=" * 50)
    
    success = test_hydrobio_taxons()
    
    if success:
        print("\n🎉 Test réussi! La nouvelle architecture fonctionne.")
    else:
        print("\n💥 Test échoué. Vérifiez la configuration.")
        sys.exit(1)
