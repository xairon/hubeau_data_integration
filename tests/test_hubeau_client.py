import asyncio

from hubeau_pipeline.assets.bronze.hubeau_client import (
    HubeauApiConfig,
    HubeauApiResponse,
    HubeauClient,
    HubeauEndpointConfig,
)


def test_fetch_all_pages_cursor(monkeypatch):
    """Les endpoints Hub'Eau paginés par curseur doivent enchaîner les pages en utilisant `next`."""

    async def _run():
        config = HubeauApiConfig(
            name="hydrometry",
            base_url="https://example.com",
            endpoints={
                "observations_tr": HubeauEndpointConfig(
                    path="observations_tr",
                    supports_cursor=True,
                    page_size=2,
                    max_pages=None,
                )
            },
        )

        responses = [
            HubeauApiResponse(
                data=[{"id": 1}, {"id": 2}],
                next="https://example.com/api/v2/hydrometrie/observations_tr?cursor=CURSOR1",
            ),
            HubeauApiResponse(
                data=[{"id": 3}],
                next=None,
            ),
        ]

        async with HubeauClient(config) as client:
            captured_params = []

            async def _fake_make_request(endpoint, params):
                captured_params.append(params)
                return responses.pop(0)

            monkeypatch.setattr(client, "_make_request", _fake_make_request)

            endpoint_config = config.endpoints["observations_tr"]
            data = await client._fetch_all_pages(
                endpoint_config,
                {"format": "json", "size": endpoint_config.page_size},
            )

        return data, captured_params

    data, captured_params = asyncio.run(_run())

    assert data == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert captured_params[0].get("cursor") is None
    assert captured_params[1]["cursor"] == "CURSOR1"


def test_fetch_all_pages_cursor_missing_token(monkeypatch):
    """Une URL `next` sans curseur ne doit pas provoquer de boucle infinie."""

    async def _run():
        config = HubeauApiConfig(
            name="hydrometry",
            base_url="https://example.com",
            endpoints={
                "observations_tr": HubeauEndpointConfig(
                    path="observations_tr",
                    supports_cursor=True,
                    page_size=2,
                    max_pages=None,
                )
            },
        )

        responses = [
            HubeauApiResponse(
                data=[{"id": 1}],
                next="https://example.com/api/v2/hydrometrie/observations_tr?page=2",
            )
        ]

        async with HubeauClient(config) as client:
            async def _fake_make_request(endpoint, params):
                return responses.pop(0)

            monkeypatch.setattr(client, "_make_request", _fake_make_request)

            endpoint_config = config.endpoints["observations_tr"]
            data = await client._fetch_all_pages(
                endpoint_config,
                {"format": "json", "size": endpoint_config.page_size},
            )

        return data

    data = asyncio.run(_run())

    assert data == [{"id": 1}]


def test_temperature_observations_split_by_month(monkeypatch):
    """L'ingestion température annuelle doit découper l'année en 12 fenêtres mensuelles."""

    async def _run():
        config = HubeauApiConfig(
            name="temperature",
            base_url="https://example.com",
            endpoints={
                "chronique": HubeauEndpointConfig(
                    path="chronique",
                    temporal_params={"start": "date_debut_mesure", "end": "date_fin_mesure"},
                    page_size=1000,
                    max_pages=20,
                )
            },
        )

        async with HubeauClient(config) as client:
            captured_windows = []

            async def _fake_fetch_all_pages(endpoint_config, params, bubble_exceptions=False):
                captured_windows.append(
                    (
                        params["date_debut_mesure"],
                        params["date_fin_mesure"],
                    )
                )
                # Simule une observation par fenêtre
                return [
                    {
                        "code_station": params["code_station"],
                        "date_mesure_temp": params["date_debut_mesure"],
                        "resultat": 12.3,
                    }
                ]

            monkeypatch.setattr(client, "_fetch_all_pages", _fake_fetch_all_pages)

            data = await client.get_temperature_observations_yearly("ST001", "2023-01-01")

        return data, captured_windows

    data, captured = asyncio.run(_run())

    assert len(captured) == 12  # 12 mois
    # Première fenêtre: janvier 2023
    assert captured[0] == ("2023-01-01", "2023-02-01")
    # Dernière fenêtre: décembre 2023 → début 2024
    assert captured[-1] == ("2023-12-01", "2024-01-01")
    # Une observation synthétique par mois → 12 au total
    assert len(data) == 12
