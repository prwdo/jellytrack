from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class Session(BaseModel):
    id: Optional[int] = None
    session_id: str
    user_id: str
    user_name: str
    device_id: str
    device_name: str
    client_name: str
    media_id: str
    media_title: str
    media_type: str
    series_name: Optional[str] = None
    season_number: Optional[int] = None
    episode_number: Optional[int] = None
    started_at: datetime
    ended_at: Optional[datetime] = None
    play_duration_seconds: int = 0
    paused_duration_seconds: int = 0
    is_active: bool = True
    last_position_seconds: int = 0
    last_state_is_paused: bool = False
    last_progress_update: datetime


class PlaybackEvent(BaseModel):
    """Model for Jellyfin playback events."""

    session_id: str
    user_id: str
    user_name: str
    device_id: str
    device_name: str
    client_name: str
    item_id: str
    item_name: str
    item_type: str
    series_name: Optional[str] = None
    season_number: Optional[int] = None
    episode_number: Optional[int] = None
    position_ticks: int = 0
    is_paused: bool = False


class UserWatchtime(BaseModel):
    """Aggregated watchtime per user."""

    user_id: str
    user_name: str
    total_seconds: int
    session_count: int


class TopMedia(BaseModel):
    """Top watched media."""

    media_id: str
    media_title: str
    media_type: str
    series_name: Optional[str] = None
    total_seconds: int
    play_count: int


class HourlyStats(BaseModel):
    """Usage stats by hour."""

    hour: int
    session_count: int
    total_seconds: int


class DeviceStats(BaseModel):
    """Device usage stats."""

    device_name: str
    client_name: str
    session_count: int
    total_seconds: int
