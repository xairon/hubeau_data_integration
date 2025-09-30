"""Tests ciblés pour le service d'ingestion Bronze Hub'Eau."""

import boto3
import pytest
from botocore.stub import Stubber, ANY

from hubeau_pipeline.assets.bronze.hubeau_client import HubeauIngestionService


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
