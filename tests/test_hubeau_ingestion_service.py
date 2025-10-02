"""Tests ciblés pour le service d'ingestion Bronze Hub'Eau."""

import asyncio
import json
from pathlib import Path

import boto3
import pytest
from botocore.stub import ANY, Stubber

from hubeau_pipeline.assets.bronze.hubeau_client import HubeauIngestionService, IngestionMetrics
from hubeau_pipeline.assets.bronze.hubeau_configs import (
    get_hydrobiology_config,
    get_temperature_config,
)


def _build_stubbed_s3_client():
    """Crée un client S3 boto3 isolé avec Stubber actif."""
    client = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    stubber = Stubber(client)
    return client, stubber


def test_save_to_minio_uses_resource_bucket():
    """Le service doit utiliser le bucket fourni par la ressource Dagster."""

    s3_client, stubber = _build_stubbed_s3_client()

    stubber.add_response("head_bucket", {}, {"Bucket": "custom-bucket"})
    stubber.add_response(
        "put_object",
        {},
        {
            "Bucket": "custom-bucket",
            "Key": "hydrometry/2024-01-01/ingestion_metadata.json",
            "Body": ANY,
            "ContentType": "application/json",
        },
    )
    stubber.add_response(
        "put_object",
        {},
        {
            "Bucket": "custom-bucket",
            "Key": "hydrometry/2024-01-01/observations_data.json",
            "Body": ANY,
            "ContentType": "application/json",
        },
    )
    stubber.add_response(
        "put_object",
        {},
        {
            "Bucket": "custom-bucket",
            "Key": "hydrometry/2024-01-01/observations_metadata.json",
            "Body": ANY,
            "ContentType": "application/json",
        },
    )

    stubber.activate()

    service = HubeauIngestionService(minio_resource={"client": s3_client, "bucket": "custom-bucket"})

    results = {
        "observations": {
            "records_count": 1,
            "data": [{"id": 1}],
        }
    }

    service._save_to_minio("hydrometry", "2024-01-01", results)

    stubber.deactivate()


def test_init_with_invalid_resource_raises():
    """Une ressource mal configurée doit lever une erreur explicite."""

    with pytest.raises(ValueError):
        HubeauIngestionService(minio_resource={"client": object()})


def test_local_fallback_is_used(tmp_path, monkeypatch):
    """Lorsque MinIO est indisponible, les données sont sauvegardées en local."""

    monkeypatch.setenv("MINIO_USER", "test")
    monkeypatch.setenv("MINIO_PASS", "test")
    monkeypatch.setenv("HUBEAU_LOCAL_CACHE", str(tmp_path))

    def _raise_minio(self):
        raise RuntimeError("minio down")

    monkeypatch.setattr(
        HubeauIngestionService,
        "_init_minio_client",
        _raise_minio,
        raising=False,
    )

    service = HubeauIngestionService()

    results = {
        "observations": {
            "records_count": 1,
            "data": [{"id": 1}],
        }
    }

    service._save_to_minio("hydrometry", "2024-01-01", results)

    expected_dir = Path(tmp_path, "hydrometry", "2024-01-01")
    metadata_file = expected_dir / "ingestion_metadata.json"
    data_file = expected_dir / "observations_data.json"

    assert metadata_file.exists()
    assert data_file.exists()

    metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
    assert metadata["total_records"] == 1


def test_ingestion_persists_metadata_for_empty_partitions(tmp_path, monkeypatch):
    """Même sans données, une partition traitée doit laisser une trace."""

    monkeypatch.setenv("MINIO_USER", "test")
    monkeypatch.setenv("MINIO_PASS", "test")
    monkeypatch.setenv("HUBEAU_LOCAL_CACHE", str(tmp_path))

    def _raise_minio(self):
        raise RuntimeError("minio indisponible")

    monkeypatch.setattr(
        HubeauIngestionService,
        "_init_minio_client",
        _raise_minio,
        raising=False,
    )

    class DummyClient:
        """Client Hub'Eau minimal qui renvoie des listes vides."""

        def __init__(self, config):
            self.metrics = IngestionMetrics()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get_stations(self, endpoint_name):
            return []

        async def get_observations(
            self,
            endpoint_name,
            entity_codes,
            date_partition,
            api_name=None,
            realtime=False,
            partition_key=None,
        ):
            return []

    monkeypatch.setattr(
        "hubeau_pipeline.assets.bronze.hubeau_client.HubeauClient",
        DummyClient,
    )

    async def _run_test():
        service = HubeauIngestionService()
        config = get_hydrobiology_config()

        result = await service.ingest_api_data(config, "2024-01-01")

        expected_dir = Path(tmp_path, config.name, "2024-01-01")
        metadata_file = expected_dir / "ingestion_metadata.json"

        assert metadata_file.exists()

        metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
        assert metadata["total_records"] == 0
        assert result["status"] == "no_data"
        assert result["errors"] == []

    asyncio.run(_run_test())


def test_temperature_ingestion_uses_monthly_strategy(monkeypatch):
    """L'ingestion température annuelle doit utiliser la stratégie mensuelle dédiée."""

    class DummyTemperatureClient:
        def __init__(self, config):
            self.metrics = IngestionMetrics()
            self.config = config

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get_stations(self, endpoint_name):
            return [{"code_station": "ST001"}]

        async def get_temperature_observations_yearly(self, station_code, date_partition):
            assert station_code == "ST001"
            assert date_partition == "2023-01-01"
            return [
                {
                    "code_station": station_code,
                    "date_mesure_temp": "2023-01-01",
                    "resultat": 15.5,
                }
            ]

        async def get_observations(
            self,
            endpoint_name,
            entity_codes,
            date_partition,
            api_name=None,
            realtime=False,
            partition_key=None,
        ):
            raise AssertionError("La stratégie mensuelle doit éviter get_observations pour les partitions annuelles")

    monkeypatch.setattr(
        "hubeau_pipeline.assets.bronze.hubeau_client.HubeauClient",
        DummyTemperatureClient,
    )

    def _noop_save(self, *args, **kwargs):
        return None

    monkeypatch.setattr(
        HubeauIngestionService,
        "_save_to_minio",
        _noop_save,
    )

    async def _run():
        service = HubeauIngestionService()
        config = get_temperature_config()
        result = await service.ingest_api_data(config, "2023-01-01", partition_key="2023")

        observations = result["results_by_endpoint"]["chronique"]
        assert observations["records_count"] == 1
        assert observations["data"][0]["resultat"] == 15.5

    asyncio.run(_run())
