"""Tests de régression ciblés pour l'API Hydrobiologie."""

import asyncio
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import List

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hubeau_pipeline.assets.bronze.hubeau_client import (
    HubeauApiResponse,
    HubeauClient,
    HubeauPageFetchError,
)
from hubeau_pipeline.assets.bronze.hubeau_configs import get_hydrobiology_config


def _build_success_response(codes: List[str]) -> HubeauApiResponse:
    """Crée une réponse simulée contenant les codes fournis."""

    payload = [
        {
            "code_station_hydrobio": code,
            "date_debut_prelevement": datetime.now(UTC).date().isoformat(),
        }
        for code in codes
    ]
    return HubeauApiResponse(data=payload, count=len(payload), next=None, previous=None)


def test_chunk_split_recovers_from_initial_failure(monkeypatch):
    """Le split binaire doit rejouer les requêtes en cas d'erreur initiale."""

    async def _run_test():
        config = get_hydrobiology_config()
        async with HubeauClient(config) as client:
            async def fake_make_request(endpoint: str, params):
                codes = params.get("code_station_hydrobio")
                if not codes:
                    return _build_success_response([])

                code_list = codes.split(",")
                if len(code_list) > 1:
                    raise RuntimeError("HTTP 500: chunk trop grand")

                return _build_success_response(code_list)

            monkeypatch.setattr(client, "_make_request", fake_make_request)

            station_codes = [f"STATION_{i:03d}" for i in range(30)]
            observations = await client.get_observations(
                "indices",
                station_codes,
                "2024-01-01",
                "hydrobiology",
            )

            assert sorted(obs["code_station_hydrobio"] for obs in observations) == station_codes
            assert client.metrics.chunks_ok == len(station_codes)
            assert client.metrics.chunks_echoues == 0
            assert client.metrics.chunks_total > len(station_codes)

    asyncio.run(_run_test())


def test_chunk_split_tracks_failing_codes(monkeypatch):
    """Les codes fautifs doivent être tracés lorsque même le split échoue."""

    async def _run_test():
        config = get_hydrobiology_config()
        async with HubeauClient(config) as client:
            async def fake_make_request(endpoint: str, params):
                codes = params.get("code_station_hydrobio")
                if not codes:
                    return _build_success_response([])

                code_list = codes.split(",")
                if len(code_list) > 1:
                    raise RuntimeError("HTTP 500: chunk trop grand")

                code = code_list[0]
                if code == "STATION_010":
                    raise RuntimeError("timeout station 10")

                return _build_success_response([code])

            monkeypatch.setattr(client, "_make_request", fake_make_request)

            station_codes = [f"STATION_{i:03d}" for i in range(30)]
            observations = await client.get_observations(
                "indices",
                station_codes,
                "2024-01-01",
                "hydrobiology",
            )

            expected = [code for code in station_codes if code != "STATION_010"]
            assert sorted(obs["code_station_hydrobio"] for obs in observations) == expected
            metrics_snapshot = client.metrics.model_dump()
            assert client.metrics.chunks_ok == len(expected), metrics_snapshot
            assert client.metrics.chunks_echoues == 1, metrics_snapshot
            assert client.metrics.codes_echoues == ["STATION_010"], metrics_snapshot

    asyncio.run(_run_test())


def test_fetch_all_pages_raises_when_first_page_fails(monkeypatch):
    """Une erreur réseau doit remonter jusqu'au service d'ingestion."""

    async def _run_test():
        config = get_hydrobiology_config()
        endpoint = config.endpoints["indices"]

        async with HubeauClient(config) as client:
            async def failing_request(endpoint_name: str, params):
                raise RuntimeError("backend indisponible")

            monkeypatch.setattr(client, "_make_request", failing_request)

            with pytest.raises(HubeauPageFetchError) as excinfo:
                await client._fetch_all_pages(endpoint, {"format": "json", "size": endpoint.page_size})

            assert "backend indisponible" in str(excinfo.value)

    asyncio.run(_run_test())
