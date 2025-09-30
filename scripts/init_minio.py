#!/usr/bin/env python3
"""
Script d'initialisation MinIO pour Hub'Eau Pipeline
Création automatique des buckets et configuration
"""

import boto3
import os
import sys
import time
from datetime import datetime

def wait_for_minio(endpoint_url, max_attempts=30):
    """Attendre que MinIO soit disponible"""
    print(f"🔄 Attente MinIO sur {endpoint_url}...")
    
    for attempt in range(max_attempts):
        try:
            client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                aws_access_key_id=os.getenv('MINIO_USER', 'admin'),
                aws_secret_access_key=os.getenv('MINIO_PASS', 'minio123'),
                region_name='us-east-1'
            )
            client.list_buckets()
            print(f"✅ MinIO disponible après {attempt + 1} tentatives")
            return client
        except Exception as e:
            if attempt < max_attempts - 1:
                print(f"⚠️ Tentative {attempt + 1}/{max_attempts} - MinIO pas encore prêt: {e}")
                time.sleep(2)
            else:
                print(f"❌ MinIO non disponible après {max_attempts} tentatives")
                return None
    
    return None

def create_bucket_if_not_exists(client, bucket_name, description=""):
    """Créer un bucket s'il n'existe pas"""
    try:
        # Tenter de créer le bucket directement
        client.create_bucket(Bucket=bucket_name)
        print(f"✅ Bucket {bucket_name} créé - {description}")
        return True
    except Exception as e:
        if 'BucketAlreadyExists' in str(e) or 'BucketAlreadyOwnedByYou' in str(e):
            print(f"✅ Bucket {bucket_name} existe déjà - {description}")
            return True
        else:
            print(f"❌ Erreur création bucket {bucket_name}: {e}")
            return False

def setup_bucket_policy(client, bucket_name):
    """Configurer politique du bucket (lecture/écriture)"""
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": ["s3:GetObject", "s3:PutObject"],
                "Resource": f"arn:aws:s3:::{bucket_name}/*"
            },
            {
                "Effect": "Allow", 
                "Principal": {"AWS": "*"},
                "Action": "s3:ListBucket",
                "Resource": f"arn:aws:s3:::{bucket_name}"
            }
        ]
    }
    
    try:
        import json
        client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=json.dumps(policy)
        )
        print(f"✅ Politique configurée pour {bucket_name}")
        return True
    except Exception as e:
        print(f"⚠️ Erreur politique bucket {bucket_name}: {e}")
        return False

def init_minio_complete():
    """Initialisation complète MinIO"""
    print("🚀 === INITIALISATION MINIO HUB'EAU ===")
    
    # Configuration
    endpoint_url = 'http://minio:9000'
    
    # Attendre MinIO
    client = wait_for_minio(endpoint_url)
    if not client:
        print("❌ Impossible de se connecter à MinIO")
        return False
    
    # Architecture simplifiée : UN bucket par couche (Bronze/Silver/Gold)
    # Utilise des préfixes pour organiser les données (hubeau/, bdlisa/, sandre/, etc.)
    buckets_config = [
        {
            'name': 'bronze',
            'description': 'Couche Bronze - Données brutes (Hub\'Eau, BDLISA, Sandre, SOSA)',
            'setup_policy': True
        },
        {
            'name': 'silver',
            'description': 'Couche Silver - Données nettoyées et transformées',
            'setup_policy': True
        },
        {
            'name': 'gold',
            'description': 'Couche Gold - Données analytiques et agrégées',
            'setup_policy': True
        }
    ]
    
    # Créer buckets
    success_count = 0
    for bucket_config in buckets_config:
        bucket_name = bucket_config['name']
        description = bucket_config['description']
        
        if create_bucket_if_not_exists(client, bucket_name, description):
            success_count += 1
            
            # Configurer politique si demandé
            if bucket_config['setup_policy']:
                setup_bucket_policy(client, bucket_name)
        
    # Test de stockage
    test_bucket = 'bronze'
    test_data = {
        'initialization': True,
        'timestamp': datetime.now().isoformat(),
        'pipeline': 'Hub\'Eau Data Integration',
        'version': '2.0',
        'architecture': 'Simplified - One bucket per layer (bronze/silver/gold)',
        'buckets_created': [b['name'] for b in buckets_config],
        'status': 'MinIO ready for production'
    }
    
    try:
        import json
        client.put_object(
            Bucket=test_bucket,
            Key='_init/minio_setup.json',
            Body=json.dumps(test_data, indent=2),
            ContentType='application/json'
        )
        print(f"✅ Test stockage dans {test_bucket}")
    except Exception as e:
        print(f"⚠️ Erreur test stockage: {e}")
    
    # Résumé
    print(f"\n📊 === RÉSUMÉ INITIALISATION ===")
    print(f"✅ Buckets créés: {success_count}/{len(buckets_config)}")
    print(f"🔗 Endpoint: {endpoint_url}")
    print(f"🎯 MinIO prêt pour pipeline Hub'Eau")
    
    return success_count == len(buckets_config)

if __name__ == "__main__":
    success = init_minio_complete()
    sys.exit(0 if success else 1)
