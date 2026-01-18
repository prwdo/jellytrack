from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

from src.database import Database
from src.models import Session


@pytest_asyncio.fixture
async def db(tmp_path):
    database = Database(tmp_path / "test.db")
    await database.connect()
    try:
        yield database
    finally:
        await database.close()


def _build_session(
    session_id: str,
    started_at: datetime,
    is_active: bool = True,
    play_duration_seconds: int = 0,
    paused_duration_seconds: int = 0,
    last_position_seconds: int = 0,
    last_state_is_paused: bool = False,
):
    return Session(
        session_id=session_id,
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
        started_at=started_at,
        ended_at=(started_at if not is_active else None),
        play_duration_seconds=play_duration_seconds,
        paused_duration_seconds=paused_duration_seconds,
        is_active=is_active,
        last_position_seconds=last_position_seconds,
        last_state_is_paused=last_state_is_paused,
        last_progress_update=started_at,
    )


@pytest.mark.asyncio
async def test_create_and_fetch_active_session(db):
    now = datetime.now()
    session = _build_session("session-1", now)
    await db.create_session(session)

    fetched = await db.get_active_session("session-1")
    assert fetched is not None
    assert fetched.session_id == "session-1"
    assert fetched.user_name == "Test User"


@pytest.mark.asyncio
async def test_timeout_stale_sessions(db):
    old = datetime.now() - timedelta(minutes=10)
    session = _build_session("session-timeout", old, is_active=True)
    await db.create_session(session)

    timed_out = await db.timeout_stale_sessions(timeout_minutes=5)
    assert timed_out == 1

    active = await db.get_active_session("session-timeout")
    assert active is None

    fetched = await db.get_session_by_id("session-timeout")
    assert fetched is not None
    assert fetched.is_active is False
    assert fetched.ended_at is not None


@pytest.mark.asyncio
async def test_aggregate_and_prune(db):
    old = datetime.now() - timedelta(days=2)
    session_a = _build_session(
        "session-a",
        old,
        is_active=False,
        play_duration_seconds=300,
        last_position_seconds=300,
    )
    session_b = _build_session(
        "session-b",
        old,
        is_active=False,
        play_duration_seconds=300,
        last_position_seconds=300,
    )
    await db.create_session(session_a)
    await db.create_session(session_b)

    pruned = await db.aggregate_and_prune(retention_days=1)
    assert pruned == 2

    cursor = await db.conn.execute("SELECT COUNT(*) as count FROM sessions")
    row = await cursor.fetchone()
    assert row["count"] == 0

    cursor = await db.conn.execute(
        "SELECT SUM(session_count) as total_sessions, SUM(play_seconds) as play_seconds "
        "FROM session_aggregates"
    )
    row = await cursor.fetchone()
    assert row["total_sessions"] == 2
    assert row["play_seconds"] == 600


@pytest.mark.asyncio
async def test_aggregate_and_prune_empty(db):
    pruned = await db.aggregate_and_prune(retention_days=0)
    assert pruned == 0


@pytest.mark.asyncio
async def test_timezone_aware_roundtrip(db):
    started_at = datetime.now(timezone.utc)
    session = _build_session("session-tz", started_at)
    await db.create_session(session)

    fetched = await db.get_session_by_id("session-tz")
    assert fetched is not None
    assert fetched.started_at.tzinfo is not None
