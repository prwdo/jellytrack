from datetime import datetime, timedelta

from fastapi.testclient import TestClient

import dashboard.routes as routes_module
from dashboard.app import app


class _DummyDB:
    @property
    def conn(self):
        raise RuntimeError("not connected")

    async def get_active_sessions(self, *_args, **_kwargs):
        return []

    async def get_user_watchtime(self, *_args, **_kwargs):
        return []

    async def get_top_media(self, *_args, **_kwargs):
        return []

    async def get_hourly_stats(self, *_args, **_kwargs):
        return []

    async def get_device_stats(self, *_args, **_kwargs):
        return []

    async def get_summary_stats(self, *_args, **_kwargs):
        return {
            "total_sessions": 0,
            "unique_users": 0,
            "unique_media": 0,
            "total_seconds": 0,
        }

    async def get_media_type_stats(self, *_args, **_kwargs):
        return []

    async def get_recent_activity(self, *_args, **_kwargs):
        return []

    async def get_daily_stats(self, *_args, **_kwargs):
        return []

    async def get_hourly_weekday_heatmap(self, *_args, **_kwargs):
        return []

    async def get_pause_ratio_by_device(self, *_args, **_kwargs):
        return []

    async def get_series_daily_totals(self, *_args, **_kwargs):
        return []

    async def get_sessions_for_metrics(self, *_args, **_kwargs):
        return []

    async def get_pause_stats(self, *_args, **_kwargs):
        return {"play_seconds": 0, "paused_seconds": 0}

    async def get_filter_options(self, *_args, **_kwargs):
        return {"users": [], "devices": [], "media_types": []}

    async def get_user_stats(self, *_args, **_kwargs):
        return {
            "user_id": "user-1",
            "user_name": "Test User",
            "total_sessions": 0,
            "total_seconds": 0,
            "unique_media": 0,
            "top_media": [],
            "recent_activity": [],
        }


class _DummyClient:
    def status(self):
        return {"connected": False, "last_message_at": None}


class _DummyDBMetrics(_DummyDB):
    async def get_daily_stats(self, *_args, **_kwargs):
        return [
            {"date": "2024-01-01", "session_count": 2, "total_seconds": 7200},
            {"date": "2024-01-02", "session_count": 3, "total_seconds": 5400},
        ]

    async def get_hourly_weekday_heatmap(self, *_args, **_kwargs):
        return [
            {"weekday": 1, "hour": 10, "watch_seconds": 10800},
            {"weekday": 5, "hour": 21, "watch_seconds": 3600},
        ]

    async def get_pause_ratio_by_device(self, *_args, **_kwargs):
        return [
            {
                "device_name": "Living Room TV",
                "client_name": "Web",
                "play_seconds": 3600,
                "paused_seconds": 600,
                "session_count": 2,
            }
        ]

    async def get_series_daily_totals(self, *_args, **_kwargs):
        return [
            {"date": "2024-01-01", "series_name": "Series A", "total_seconds": 3600},
            {"date": "2024-01-02", "series_name": "Series A", "total_seconds": 1800},
        ]

    async def get_sessions_for_metrics(self, *_args, **_kwargs):
        start = datetime(2024, 1, 1, 10, 0, 0)
        end = start + timedelta(minutes=30)
        return [
            {
                "session_id": "s1",
                "media_id": "m1",
                "media_type": "Movie",
                "started_at": start.isoformat(),
                "ended_at": end.isoformat(),
                "is_active": False,
                "play_seconds": 200,
                "paused_seconds": 0,
                "last_position_seconds": 1200,
                "last_progress_update": end.isoformat(),
            },
            {
                "session_id": "s2",
                "media_id": "m1",
                "media_type": "Movie",
                "started_at": start.isoformat(),
                "ended_at": end.isoformat(),
                "is_active": False,
                "play_seconds": 600,
                "paused_seconds": 0,
                "last_position_seconds": 900,
                "last_progress_update": end.isoformat(),
            },
            {
                "session_id": "s3",
                "media_id": "m2",
                "media_type": "Episode",
                "started_at": start.isoformat(),
                "ended_at": end.isoformat(),
                "is_active": False,
                "play_seconds": 2000,
                "paused_seconds": 0,
                "last_position_seconds": 700,
                "last_progress_update": end.isoformat(),
            },
            {
                "session_id": "s4",
                "media_id": "m3",
                "media_type": "Movie",
                "started_at": start.isoformat(),
                "ended_at": end.isoformat(),
                "is_active": False,
                "play_seconds": 4000,
                "paused_seconds": 0,
                "last_position_seconds": 4000,
                "last_progress_update": end.isoformat(),
            },
            {
                "session_id": "s5",
                "media_id": "m4",
                "media_type": "Movie",
                "started_at": start.isoformat(),
                "ended_at": end.isoformat(),
                "is_active": False,
                "play_seconds": 8000,
                "paused_seconds": 0,
                "last_position_seconds": 8000,
                "last_progress_update": end.isoformat(),
            },
        ]


def test_index_route_renders(monkeypatch):
    monkeypatch.setattr(routes_module, "db", _DummyDB())
    monkeypatch.setattr(routes_module, "jellyfin_client", _DummyClient())

    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200


def test_user_route_renders(monkeypatch):
    monkeypatch.setattr(routes_module, "db", _DummyDB())
    monkeypatch.setattr(routes_module, "jellyfin_client", _DummyClient())

    client = TestClient(app)
    response = client.get("/user/user-1")
    assert response.status_code == 200


def test_health_route(monkeypatch):
    monkeypatch.setattr(routes_module, "db", _DummyDB())
    monkeypatch.setattr(routes_module, "jellyfin_client", _DummyClient())

    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"


def test_hourly_stats_route(monkeypatch):
    monkeypatch.setattr(routes_module, "db", _DummyDB())
    monkeypatch.setattr(routes_module, "jellyfin_client", _DummyClient())

    client = TestClient(app)
    response = client.get("/api/stats/hourly")
    assert response.status_code == 200
    assert response.json() == []


def test_index_route_renders_metrics_charts(monkeypatch):
    monkeypatch.setattr(routes_module, "db", _DummyDBMetrics())
    monkeypatch.setattr(routes_module, "jellyfin_client", _DummyClient())

    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    body = response.text
    assert "const heatmapMax = 10800" in body
    assert "data: [1, 1, 0, 1, 1, 1]" in body
    assert "Series A" in body
