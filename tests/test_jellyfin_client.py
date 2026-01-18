import json
from datetime import datetime, timedelta

import pytest

import src.jellyfin_client as jellyfin_client_module
from src.jellyfin_client import JellyfinWebSocketClient
from src.models import Session


def _build_session(last_update: datetime, last_position: int, last_paused: bool) -> Session:
    return Session(
        session_id="session-1",
        user_id="user-1",
        user_name="Test User",
        device_id="device-1",
        device_name="Test Device",
        client_name="Test Client",
        media_id="media-1",
        media_title="Test Media",
        media_type="Movie",
        series_name=None,
        season_number=None,
        episode_number=None,
        started_at=last_update,
        ended_at=None,
        play_duration_seconds=0,
        paused_duration_seconds=0,
        is_active=True,
        last_position_seconds=last_position,
        last_state_is_paused=last_paused,
        last_progress_update=last_update,
    )


class _DummyDB:
    def __init__(self, existing: Session | None = None):
        self.existing = existing
        self.updated = []
        self.ended = []
        self.created = []

    async def get_active_session(self, session_id: str) -> Session | None:
        if self.existing and self.existing.session_id == session_id:
            return self.existing
        return None

    async def get_active_sessions(self):
        return []

    async def update_session_state(
        self,
        session_id: str,
        position_seconds: int,
        is_paused: bool,
        play_add_seconds: int,
        paused_add_seconds: int,
        now: datetime,
    ) -> None:
        self.updated.append(
            {
                "session_id": session_id,
                "position_seconds": position_seconds,
                "is_paused": is_paused,
                "play_add_seconds": play_add_seconds,
                "paused_add_seconds": paused_add_seconds,
            }
        )

    async def end_session(self, session_id: str) -> None:
        self.ended.append(session_id)

    async def create_session(self, session: Session) -> None:
        self.created.append(session)


def test_calculate_deltas_paused_then_playing():
    client = JellyfinWebSocketClient()
    last_update = datetime.now() - timedelta(seconds=10)
    existing = _build_session(last_update, last_position=50, last_paused=True)
    now = last_update + timedelta(seconds=10)

    play_add, paused_add = client._calculate_deltas(
        existing, position_seconds=70, is_paused=False, now=now
    )

    assert paused_add == 10
    assert play_add == 20


def test_calculate_deltas_while_paused():
    client = JellyfinWebSocketClient()
    last_update = datetime.now() - timedelta(seconds=10)
    existing = _build_session(last_update, last_position=50, last_paused=False)
    now = last_update + timedelta(seconds=10)

    play_add, paused_add = client._calculate_deltas(
        existing, position_seconds=70, is_paused=True, now=now
    )

    assert paused_add == 0
    assert play_add == 0


@pytest.mark.asyncio
async def test_handle_sessions_allows_null_position_ticks(monkeypatch):
    client = JellyfinWebSocketClient()
    existing = _build_session(
        datetime.now() - timedelta(seconds=5), last_position=10, last_paused=False
    )
    dummy_db = _DummyDB(existing=existing)
    monkeypatch.setattr(jellyfin_client_module, "db", dummy_db)

    sessions = [
        {
            "Id": "session-1",
            "UserId": "user-1",
            "UserName": "User",
            "DeviceId": "device-1",
            "DeviceName": "Device",
            "Client": "Client",
            "NowPlayingItem": {"Id": "media-1", "Name": "Title", "Type": "Movie"},
            "PlayState": {"PositionTicks": None, "IsPaused": False},
        }
    ]

    await client._handle_sessions(sessions)

    assert len(dummy_db.updated) == 1
    assert dummy_db.updated[0]["position_seconds"] == 0


@pytest.mark.asyncio
async def test_playback_start_missing_item_is_noop(monkeypatch):
    client = JellyfinWebSocketClient()
    dummy_db = _DummyDB()
    monkeypatch.setattr(jellyfin_client_module, "db", dummy_db)

    created = {"count": 0}

    async def _fake_create_session(*_args, **_kwargs):
        created["count"] += 1

    monkeypatch.setattr(client, "_create_session", _fake_create_session)

    message = {
        "MessageType": "PlaybackStart",
        "Data": {"SessionId": "session-1"},
    }
    await client._handle_message(json.dumps(message))

    assert created["count"] == 0


@pytest.mark.asyncio
async def test_playback_stopped_handles_null_position_ticks(monkeypatch):
    client = JellyfinWebSocketClient()
    existing = _build_session(
        datetime.now() - timedelta(seconds=5), last_position=10, last_paused=False
    )
    dummy_db = _DummyDB(existing=existing)
    monkeypatch.setattr(jellyfin_client_module, "db", dummy_db)

    message = {
        "MessageType": "PlaybackStopped",
        "Data": {
            "SessionId": "session-1",
            "PlayState": {"PositionTicks": None, "IsPaused": False},
        },
    }
    await client._handle_message(json.dumps(message))

    assert dummy_db.ended == ["session-1"]
