#!/usr/bin/env python3
"""
Script de monitoring des données Hub'Eau dans MinIO
Analyse les fichiers JSONL compressés et affiche des statistiques détaillées
"""
import subprocess
import json
import gzip
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Any
import sys


def run_docker_mc_command(cmd: str) -> bytes:
    """Exécute une commande mc via docker exec"""
    full_cmd = f'docker exec brgm-minio-1 mc {cmd}'
    result = subprocess.run(full_cmd, shell=True, capture_output=True)
    if result.returncode != 0:
        print(f"Erreur: {result.stderr.decode('utf-8', errors='ignore')}")
        return b""
    return result.stdout


def list_datasets(bucket: str = 'bronze') -> List[str]:
    """Liste tous les datasets (API) dans le bucket"""
    output = run_docker_mc_command(f'ls local/{bucket}/')
    if not output:
        return []

    datasets = []
    for line in output.decode('utf-8').split('\n'):
        if line.strip() and 'DIR' not in line and '_api/' in line:
            # Extraire le nom du dataset
            parts = line.strip().split()
            if parts:
                dataset = parts[-1].rstrip('/')
                if dataset and dataset not in ['init', '_dlt_loads', '_dlt_pipeline_state', '_dlt_version']:
                    datasets.append(dataset)

    return sorted(set(datasets))


def list_tables(bucket: str, dataset: str) -> List[str]:
    """Liste toutes les tables dans un dataset"""
    output = run_docker_mc_command(f'ls local/{bucket}/{dataset}/')
    if not output:
        return []

    tables = []
    for line in output.decode('utf-8').split('\n'):
        if line.strip():
            parts = line.strip().split()
            if parts:
                table = parts[-1].rstrip('/')
                if table and not table.startswith('_dlt'):
                    tables.append(table)

    return sorted(set(tables))


def list_partitions(bucket: str, dataset: str, table: str) -> List[str]:
    """Liste toutes les partitions (dates) dans une table"""
    output = run_docker_mc_command(f'ls local/{bucket}/{dataset}/{table}/')
    if not output:
        return []

    partitions = []
    for line in output.decode('utf-8').split('\n'):
        if line.strip():
            parts = line.strip().split()
            if parts:
                partition = parts[-1].rstrip('/')
                if partition:
                    partitions.append(partition)

    return sorted(set(partitions))


def analyze_file(bucket: str, file_path: str) -> Dict[str, Any]:
    """Analyse un fichier JSONL compressé"""
    output = run_docker_mc_command(f'cat local/{bucket}/{file_path}')
    if not output:
        return {"error": "Impossible de lire le fichier", "records": 0}

    try:
        # Décompresser
        decompressed = gzip.decompress(output)
        lines = decompressed.decode('utf-8').strip().split('\n')

        records = []
        for line in lines:
            if line.strip():
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

        if not records:
            return {"records": 0, "size_bytes": len(output), "size_decompressed": len(decompressed)}

        # Analyser les champs
        fields = set()
        sample_record = records[0]

        for record in records[:100]:  # Analyser les 100 premiers pour les champs
            fields.update(record.keys())

        # Détecter les champs clés importants
        key_fields = {
            'code_station', 'code_bss', 'code_ouvrage',
            'date_mesure', 'date_prelevement', 'date_obs', 'date_mesure_temp',
            'timestamp_mesure', 'date_operation'
        }
        detected_keys = list(fields & key_fields)

        return {
            "records": len(records),
            "size_bytes": len(output),
            "size_decompressed": len(decompressed),
            "compression_ratio": len(output) / len(decompressed) if len(decompressed) > 0 else 0,
            "fields_count": len(fields),
            "detected_keys": detected_keys,
            "sample_fields": list(fields)[:10],
            "sample_record": sample_record
        }

    except Exception as e:
        return {"error": str(e), "records": 0, "size_bytes": len(output)}


def format_size(bytes_size: int) -> str:
    """Formate une taille en bytes de manière lisible"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"


def monitor_dataset(dataset: str, bucket: str = 'bronze', show_details: bool = False):
    """Monitore un dataset spécifique"""
    print(f"\n{'='*80}")
    print(f"ANALYSE DU DATASET: {dataset}")
    print(f"{'='*80}")

    tables = list_tables(bucket, dataset)
    if not tables:
        print(f"  Aucune table trouvée dans {dataset}")
        return

    total_records = 0
    total_size = 0

    for table in tables:
        print(f"\n  TABLE: {table}")
        print(f"  {'-'*76}")

        partitions = list_partitions(bucket, dataset, table)

        if not partitions:
            print(f"    Aucune partition")
            continue

        table_records = 0
        table_size = 0

        for partition in partitions:
            # Analyser le fichier data.json
            file_path = f"{dataset}/{table}/{partition}/data.json"
            stats = analyze_file(bucket, file_path)

            records = stats.get('records', 0)
            size = stats.get('size_bytes', 0)

            table_records += records
            table_size += size

            compression = stats.get('compression_ratio', 0)

            print(f"    Partition {partition}:")
            print(f"      Records: {records:,}")
            print(f"      Taille compressée: {format_size(size)}")
            if compression > 0:
                print(f"      Compression: {compression*100:.1f}% (ratio {1/compression:.1f}x)")

            if show_details and 'detected_keys' in stats:
                print(f"      Clés détectées: {', '.join(stats['detected_keys'])}")

            if 'error' in stats:
                print(f"      Erreur: {stats['error']}")

        print(f"\n    TOTAL TABLE {table}: {table_records:,} records, {format_size(table_size)}")

        total_records += table_records
        total_size += table_size

    print(f"\n  {'='*76}")
    print(f"  TOTAL DATASET {dataset}: {total_records:,} records, {format_size(total_size)}")
    print(f"  {'='*76}")


def monitor_all(bucket: str = 'bronze', show_details: bool = False):
    """Monitore tous les datasets"""
    datasets = list_datasets(bucket)

    if not datasets:
        print(f"Aucun dataset trouvé dans le bucket '{bucket}'")
        return

    print(f"\n{'#'*80}")
    print(f"# MONITORING MINIO - Bucket: {bucket}")
    print(f"# Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"# Datasets trouvés: {len(datasets)}")
    print(f"{'#'*80}")

    summary = []

    for dataset in datasets:
        tables = list_tables(bucket, dataset)

        dataset_records = 0
        dataset_size = 0

        for table in tables:
            partitions = list_partitions(bucket, dataset, table)

            for partition in partitions:
                file_path = f"{dataset}/{table}/{partition}/data.json"
                stats = analyze_file(bucket, file_path)

                dataset_records += stats.get('records', 0)
                dataset_size += stats.get('size_bytes', 0)

        summary.append({
            'dataset': dataset,
            'tables': len(tables),
            'records': dataset_records,
            'size': dataset_size
        })

        if show_details:
            monitor_dataset(dataset, bucket, show_details)

    # Afficher le résumé
    print(f"\n{'='*80}")
    print("RÉSUMÉ GLOBAL")
    print(f"{'='*80}")
    print(f"{'Dataset':<40} {'Tables':<10} {'Records':<15} {'Taille':<15}")
    print(f"{'-'*80}")

    total_records = 0
    total_size = 0

    for item in sorted(summary, key=lambda x: x['records'], reverse=True):
        print(f"{item['dataset']:<40} {item['tables']:<10} {item['records']:>14,} {format_size(item['size']):>14}")
        total_records += item['records']
        total_size += item['size']

    print(f"{'-'*80}")
    print(f"{'TOTAL':<40} {len(summary):<10} {total_records:>14,} {format_size(total_size):>14}")
    print(f"{'='*80}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Monitoring des données Hub\'Eau dans MinIO')
    parser.add_argument('--bucket', default='bronze', help='Nom du bucket MinIO (défaut: bronze)')
    parser.add_argument('--dataset', help='Analyser un dataset spécifique')
    parser.add_argument('--details', action='store_true', help='Afficher les détails complets')

    args = parser.parse_args()

    if args.dataset:
        monitor_dataset(args.dataset, args.bucket, args.details)
    else:
        monitor_all(args.bucket, args.details)
