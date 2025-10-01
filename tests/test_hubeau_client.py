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
