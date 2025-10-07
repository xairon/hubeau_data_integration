"""
Assets Dagster pour le monitoring des données MinIO
"""
from typing import Any, Dict
import subprocess
import json
import gzip
from datetime import datetime
from collections import defaultdict

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    Output
)


def run_docker_mc_command(cmd: str) -> bytes:
    """Exécute une commande mc via docker exec"""
    full_cmd = f'docker exec brgm-minio-1 mc {cmd}'
    result = subprocess.run(full_cmd, shell=True, capture_output=True)
    if result.returncode != 0:
        return b""
    return result.stdout


def list_datasets(bucket: str = 'bronze') -> list[str]:
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


def list_tables(bucket: str, dataset: str) -> list[str]:
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


def list_partitions(bucket: str, dataset: str, table: str) -> list[str]:
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
        for record in records[:1000]:
            for field in date_fields:
                if field in record and record[field]:
                    dates.append(str(record[field])[:10])

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


def format_size(bytes_size: int) -> str:
    """Formate une taille en bytes de manière lisible"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"


@asset(
    group_name="monitoring",
    description="Génère un rapport de monitoring complet des données dans MinIO",
)
def minio_data_quality_report(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Asset de monitoring qui analyse toutes les données stockées dans MinIO
    et génère un rapport détaillé avec métriques
    """
    bucket = 'bronze'
    datasets = list_datasets(bucket)

    context.log.info(f"Analyse de {len(datasets)} datasets dans le bucket '{bucket}'")

    report = {
        "timestamp": datetime.now().isoformat(),
        "bucket": bucket,
        "datasets_count": len(datasets),
        "datasets": []
    }

    total_records = 0
    total_size = 0
    dataset_summaries = {}

    for dataset in datasets:
        context.log.info(f"Analyse du dataset: {dataset}")

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

        dataset_summaries[dataset] = {
            'records': dataset_records,
            'size': dataset_size,
            'tables': len(tables)
        }

        context.log.info(f"  Dataset {dataset}: {dataset_records:,} records, {format_size(dataset_size)}")

    report['total_records'] = total_records
    report['total_size_bytes'] = total_size

    # Créer un résumé formaté pour les métadonnées
    summary_md = "# Résumé du Monitoring MinIO\n\n"
    summary_md += f"**Timestamp:** {report['timestamp']}\n\n"
    summary_md += f"**Bucket:** {bucket}\n\n"
    summary_md += "## Statistiques Globales\n\n"
    summary_md += f"- **Total Records:** {total_records:,}\n"
    summary_md += f"- **Taille Totale:** {format_size(total_size)}\n"
    summary_md += f"- **Datasets:** {len(datasets)}\n\n"
    summary_md += "## Détails par Dataset\n\n"
    summary_md += "| Dataset | Tables | Records | Taille |\n"
    summary_md += "|---------|--------|---------|--------|\n"

    for dataset, info in sorted(dataset_summaries.items(), key=lambda x: x[1]['records'], reverse=True):
        summary_md += f"| {dataset} | {info['tables']} | {info['records']:,} | {format_size(info['size'])} |\n"

    context.log.info(f"Rapport généré: {total_records:,} records, {format_size(total_size)}")

    return Output(
        value=report,
        metadata={
            "total_records": MetadataValue.int(total_records),
            "total_size_bytes": MetadataValue.int(total_size),
            "total_size_formatted": MetadataValue.text(format_size(total_size)),
            "datasets_count": MetadataValue.int(len(datasets)),
            "summary": MetadataValue.md(summary_md),
            "report_json": MetadataValue.json(report)
        }
    )


@asset(
    group_name="monitoring",
    description="Vérifie la qualité des données et détecte les anomalies",
)
def minio_data_quality_checks(
    context: AssetExecutionContext,
    minio_data_quality_report: Dict[str, Any]
) -> Output[Dict[str, Any]]:
    """
    Analyse le rapport de monitoring et détecte des anomalies potentielles
    """
    checks = {
        "timestamp": datetime.now().isoformat(),
        "checks": [],
        "warnings": [],
        "errors": []
    }

    # Vérifier les datasets vides
    for dataset_info in minio_data_quality_report['datasets']:
        dataset_name = dataset_info['name']
        total_records = dataset_info['total_records']

        if total_records == 0:
            checks['warnings'].append(f"Dataset '{dataset_name}' est vide (0 records)")

        # Vérifier les tables vides
        for table_info in dataset_info['tables']:
            table_name = table_info['name']
            table_records = table_info['total_records']

            if table_records == 0 and not table_name.startswith('_'):
                checks['warnings'].append(f"Table '{dataset_name}/{table_name}' est vide")

        # Vérifier les ratios de compression anormaux
        for table_info in dataset_info['tables']:
            for partition_info in table_info['partitions']:
                ratio = partition_info.get('compression_ratio', 0)
                if ratio > 0.5:  # Compression < 50% (mauvaise compression)
                    checks['warnings'].append(
                        f"Ratio de compression faible pour {dataset_name}/{table_info['name']}/{partition_info['date']}: {ratio*100:.1f}%"
                    )

                # Vérifier les erreurs
                if 'error' in partition_info:
                    checks['errors'].append(
                        f"Erreur pour {dataset_name}/{table_info['name']}/{partition_info['date']}: {partition_info['error']}"
                    )

    checks['checks'].append({
        'name': 'empty_datasets',
        'count': len([w for w in checks['warnings'] if 'Dataset' in w and 'vide' in w])
    })

    checks['checks'].append({
        'name': 'compression_issues',
        'count': len([w for w in checks['warnings'] if 'compression' in w])
    })

    checks['checks'].append({
        'name': 'errors',
        'count': len(checks['errors'])
    })

    status = "✅ PASS" if len(checks['errors']) == 0 else "❌ FAIL"
    warning_count = len(checks['warnings'])

    context.log.info(f"Quality checks: {status}")
    context.log.info(f"Warnings: {warning_count}")
    context.log.info(f"Errors: {len(checks['errors'])}")

    return Output(
        value=checks,
        metadata={
            "status": MetadataValue.text(status),
            "warnings_count": MetadataValue.int(warning_count),
            "errors_count": MetadataValue.int(len(checks['errors'])),
            "checks_json": MetadataValue.json(checks)
        }
    )
