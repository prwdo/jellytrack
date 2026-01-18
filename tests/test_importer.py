from datetime import datetime, timezone

import pytest
import pytest_asyncio

import src.importer as importer_module
from src.database import Database
from src.importer import PlaybackReportingImporter


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | list):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class _FakeAsyncClient:
    def __init__(self, post_response: _FakeResponse, get_response: _FakeResponse):
        self._post_response = post_response
        self._get_response = get_response

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, *_args, **_kwargs):
        return self._post_response

    async def get(self, *_args, **_kwargs):
        return self._get_response


@pytest_asyncio.fixture
async def db(tmp_path):
    database = Database(tmp_path / "import.db")
    await database.connect()
    try:
        yield database
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_importer_parses_dates_and_series(monkeypatch, db):
    columns = [
        "rowid",
        "DateCreated",
        "UserId",
        "ItemId",
        "ItemType",
        "ItemName",
        "PlaybackMethod",
        "ClientName",
        "DeviceName",
        "PlayDuration",
    ]
    results = [
        [
            1,
            "2024-01-02T03:04:05Z",
            "user-1",
            "item-1",
            "Episode",
            "Series - s01e02 - Episode Title",
            "DirectPlay",
            "Client",
            "Device",
            120,
        ],
        [
            2,
            "2024-01-02 03:04:05.123",
            "user-1",
            "item-2",
            "Movie",
            "Some Movie",
            "DirectPlay",
            "Client",
            "Device",
            300,
        ],
        [
            3,
            "2024-01-02 03:04:05",
            "user-1",
            "item-3",
            "Movie",
            "Other Movie",
            "DirectPlay",
            "Client",
            "Device",
            60,
        ],
    ]
    post_response = _FakeResponse(200, {"columns": columns, "results": results})
    get_response = _FakeResponse(200, [{"Id": "user-1", "Name": "Alice"}])

    def _fake_async_client():
        return _FakeAsyncClient(post_response, get_response)

    monkeypatch.setattr(importer_module, "db", db)
    monkeypatch.setattr(importer_module.httpx, "AsyncClient", _fake_async_client)
    monkeypatch.setattr(importer_module.settings, "jellyfin_url", "http://example.test")
    monkeypatch.setattr(importer_module.settings, "jellyfin_api_key", "key")

    importer = PlaybackReportingImporter()
    imported = await importer.import_all(days=7)

    assert imported == 3

    session = await db.get_session_by_id("imported_1")
    assert session is not None
    assert session.user_name == "Alice"
    assert session.series_name == "Series"
    assert session.season_number == 1
    assert session.episode_number == 2

    session_aware = await db.get_session_by_id("imported_1")
    assert session_aware.started_at.tzinfo is not None
    assert session_aware.started_at == datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
