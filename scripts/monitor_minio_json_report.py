#!/usr/bin/env python3
"""
Génère un rapport JSON détaillé du monitoring MinIO
Pour intégration avec Dagster ou autres outils
"""
import subprocess
import json
import gzip
from datetime import datetime
from typing import Dict, List, Any
import sys


def run_docker_mc_command(cmd: str) -> bytes:
    """Exécute une commande mc via docker exec"""
    full_cmd = f'docker exec brgm-minio-1 mc {cmd}'
    result = subprocess.run(full_cmd, shell=True, capture_output=True)
    if result.returncode != 0:
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

        fields = set()
        for record in records[:100]:
            fields.update(record.keys())

        key_fields = {
            'code_station', 'code_bss', 'code_ouvrage',
            'date_mesure', 'date_prelevement', 'date_obs', 'date_mesure_temp',
            'timestamp_mesure', 'date_operation'
        }
        detected_keys = list(fields & key_fields)

        # Extraire les dates pour analyser la période couverte
        date_fields = ['date_mesure', 'date_prelevement', 'date_obs', 'date_mesure_temp', 'timestamp_mesure', 'date_operation']
        dates = []
        for record in records[:1000]:  # Analyser 1000 premiers records
            for field in date_fields:
                if field in record and record[field]:
                    dates.append(str(record[field])[:10])  # Prendre juste la date (YYYY-MM-DD)

        date_range = {}
        if dates:
            date_range = {
                'min': min(dates),
                'max': max(dates),
                'count': len(set(dates))
            }

        return {
            "records": len(records),
            "size_bytes": len(output),
            "size_decompressed": len(decompressed),
            "compression_ratio": len(output) / len(decompressed) if len(decompressed) > 0 else 0,
            "fields_count": len(fields),
            "detected_keys": detected_keys,
            "date_range": date_range
        }

    except Exception as e:
        return {"error": str(e), "records": 0, "size_bytes": len(output)}


def generate_report(bucket: str = 'bronze') -> Dict[str, Any]:
    """Génère un rapport complet en JSON"""
    datasets = list_datasets(bucket)

    report = {
        "timestamp": datetime.now().isoformat(),
        "bucket": bucket,
        "datasets_count": len(datasets),
        "datasets": []
    }

    total_records = 0
    total_size = 0

    for dataset in datasets:
        dataset_info = {
            "name": dataset,
            "tables": []
        }

        tables = list_tables(bucket, dataset)
        dataset_records = 0
        dataset_size = 0

        for table in tables:
            table_info = {
                "name": table,
                "partitions": []
            }

            partitions = list_partitions(bucket, dataset, table)
            table_records = 0
            table_size = 0

            for partition in partitions:
                file_path = f"{dataset}/{table}/{partition}/data.json"
                stats = analyze_file(bucket, file_path)

                partition_info = {
                    "date": partition,
                    "records": stats.get('records', 0),
                    "size_bytes": stats.get('size_bytes', 0),
                    "size_decompressed": stats.get('size_decompressed', 0),
                    "compression_ratio": stats.get('compression_ratio', 0),
                    "fields_count": stats.get('fields_count', 0),
                    "detected_keys": stats.get('detected_keys', []),
                    "date_range": stats.get('date_range', {})
                }

                if 'error' in stats:
                    partition_info['error'] = stats['error']

                table_info['partitions'].append(partition_info)
                table_records += stats.get('records', 0)
                table_size += stats.get('size_bytes', 0)

            table_info['total_records'] = table_records
            table_info['total_size_bytes'] = table_size

            dataset_info['tables'].append(table_info)
            dataset_records += table_records
            dataset_size += table_size

        dataset_info['total_records'] = dataset_records
        dataset_info['total_size_bytes'] = dataset_size

        report['datasets'].append(dataset_info)
        total_records += dataset_records
        total_size += dataset_size

    report['total_records'] = total_records
    report['total_size_bytes'] = total_size

    return report


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Génère un rapport JSON du monitoring MinIO')
    parser.add_argument('--bucket', default='bronze', help='Nom du bucket MinIO (défaut: bronze)')
    parser.add_argument('--output', help='Fichier de sortie (défaut: stdout)')
    parser.add_argument('--pretty', action='store_true', help='JSON formaté (pretty-print)')

    args = parser.parse_args()

    report = generate_report(args.bucket)

    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2 if args.pretty else None, ensure_ascii=False)
        print(f"Rapport généré: {args.output}", file=sys.stderr)
    else:
        print(json.dumps(report, indent=2 if args.pretty else None, ensure_ascii=False))
