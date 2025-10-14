#!/usr/bin/env python3
"""
Script pour nettoyer les buckets MinIO et consolider sur un seul bucket 'bronze' avec parquet.

Ce script :
1. Liste tous les buckets MinIO
2. Supprime le bucket 'hubeau-bronze' s'il existe
3. Garde uniquement le bucket 'bronze'
4. Vérifie que les données sont au format parquet

Usage:
    python scripts/cleanup_minio_buckets.py
"""

import os
from minio import Minio
from minio.error import S3Error


def get_minio_client():
    """Crée un client MinIO depuis les variables d'environnement."""
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", "").replace("https://", "")
    access_key = os.getenv("MINIO_USER", "admin")
    secret_key = os.getenv("MINIO_PASS", "BrgmMinio2024!")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    
    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )


def list_buckets(client):
    """Liste tous les buckets."""
    buckets = client.list_buckets()
    print(f"\n📦 Buckets existants ({len(buckets)}):")
    for bucket in buckets:
        print(f"  - {bucket.name} (créé le {bucket.creation_date})")
    return buckets


def count_objects_in_bucket(client, bucket_name):
    """Compte les objets dans un bucket."""
    try:
        objects = list(client.list_objects(bucket_name, recursive=True))
        return len(objects)
    except S3Error:
        return 0


def list_bucket_contents(client, bucket_name, max_items=10):
    """Liste le contenu d'un bucket (limité à max_items)."""
    try:
        objects = client.list_objects(bucket_name, recursive=True)
        print(f"\n📂 Contenu du bucket '{bucket_name}':")
        
        count = 0
        parquet_count = 0
        jsonl_count = 0
        other_count = 0
        
        for i, obj in enumerate(objects):
            if i < max_items:
                print(f"  - {obj.object_name} ({obj.size} bytes)")
            
            count += 1
            if obj.object_name.endswith('.parquet'):
                parquet_count += 1
            elif obj.object_name.endswith('.jsonl'):
                jsonl_count += 1
            else:
                other_count += 1
        
        if count > max_items:
            print(f"  ... et {count - max_items} autres fichiers")
        
        print(f"\n📊 Statistiques:")
        print(f"  - Total: {count} fichiers")
        print(f"  - Parquet: {parquet_count}")
        print(f"  - JSONL: {jsonl_count}")
        print(f"  - Autres: {other_count}")
        
    except S3Error as e:
        print(f"❌ Erreur lors de la lecture du bucket '{bucket_name}': {e}")


def delete_bucket_recursive(client, bucket_name):
    """Supprime un bucket et tout son contenu."""
    try:
        # D'abord supprimer tous les objets
        objects = client.list_objects(bucket_name, recursive=True)
        for obj in objects:
            client.remove_object(bucket_name, obj.object_name)
            print(f"  🗑️ Supprimé: {obj.object_name}")
        
        # Puis supprimer le bucket
        client.remove_bucket(bucket_name)
        print(f"✅ Bucket '{bucket_name}' supprimé avec succès")
        return True
    except S3Error as e:
        print(f"❌ Erreur lors de la suppression du bucket '{bucket_name}': {e}")
        return False


def main():
    """Point d'entrée principal."""
    print("🧹 Script de nettoyage des buckets MinIO")
    print("=" * 60)
    
    # Créer le client MinIO
    try:
        client = get_minio_client()
        print("✅ Connexion MinIO établie")
    except Exception as e:
        print(f"❌ Impossible de se connecter à MinIO: {e}")
        return
    
    # Lister les buckets existants
    buckets = list_buckets(client)
    
    # Analyser chaque bucket
    for bucket in buckets:
        list_bucket_contents(client, bucket.name, max_items=5)
    
    # Identifier les buckets à nettoyer
    buckets_to_delete = []
    for bucket in buckets:
        if bucket.name in ['hubeau-bronze', 'bronze-bucket']:
            buckets_to_delete.append(bucket.name)
    
    if not buckets_to_delete:
        print("\n✅ Aucun bucket à nettoyer, configuration déjà correcte!")
        return
    
    print(f"\n⚠️ Buckets à supprimer: {', '.join(buckets_to_delete)}")
    
    # Demander confirmation
    response = input("\n❓ Voulez-vous supprimer ces buckets ? (yes/no): ")
    if response.lower() not in ['yes', 'y', 'oui', 'o']:
        print("❌ Annulation")
        return
    
    # Supprimer les buckets
    for bucket_name in buckets_to_delete:
        print(f"\n🗑️ Suppression du bucket '{bucket_name}'...")
        delete_bucket_recursive(client, bucket_name)
    
    # Vérifier que le bucket 'bronze' existe
    if not client.bucket_exists('bronze'):
        print("\n📦 Création du bucket 'bronze'...")
        client.make_bucket('bronze')
        print("✅ Bucket 'bronze' créé")
    
    # Résumé final
    print("\n" + "=" * 60)
    print("🎉 Nettoyage terminé!")
    print("\n📦 Configuration finale:")
    final_buckets = list_buckets(client)
    
    for bucket in final_buckets:
        count = count_objects_in_bucket(client, bucket.name)
        print(f"  ✅ {bucket.name}: {count} fichiers")


if __name__ == "__main__":
    main()

