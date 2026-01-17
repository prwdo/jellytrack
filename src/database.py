import aiosqlite
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path

from .config import settings
from .models import (
    Session,
    UserWatchtime,
    TopMedia,
    HourlyStats,
    DeviceStats,
)


class Database:
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or settings.database_path_resolved
        self._connection: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        """Connect to the database and create tables if needed."""
        self._connection = await aiosqlite.connect(self.db_path)
        self._connection.row_factory = aiosqlite.Row
        await self._create_tables()

    async def close(self) -> None:
        """Close the database connection."""
        if self._connection:
            await self._connection.close()
            self._connection = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if not self._connection:
            raise RuntimeError("Database not connected")
        return self._connection

    async def _create_tables(self) -> None:
        """Create database tables if they don't exist."""
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT UNIQUE,
                user_id TEXT,
                user_name TEXT,
                device_id TEXT,
                device_name TEXT,
                client_name TEXT,
                media_id TEXT,
                media_title TEXT,
                media_type TEXT,
                series_name TEXT,
                season_number INTEGER,
                episode_number INTEGER,
                started_at TIMESTAMP,
                ended_at TIMESTAMP,
                play_duration_seconds INTEGER DEFAULT 0,
                is_active BOOLEAN DEFAULT TRUE,
                last_progress_update TIMESTAMP
            )
        """)
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sessions_active ON sessions(is_active)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sessions_started ON sessions(started_at)"
        )
        await self.conn.commit()

    async def create_session(self, session: Session) -> int:
        """Create a new playback session."""
        cursor = await self.conn.execute(
            """
            INSERT INTO sessions (
                session_id, user_id, user_name, device_id, device_name,
                client_name, media_id, media_title, media_type, series_name,
                season_number, episode_number, started_at, ended_at,
                play_duration_seconds, is_active, last_progress_update
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session.session_id,
                session.user_id,
                session.user_name,
                session.device_id,
                session.device_name,
                session.client_name,
                session.media_id,
                session.media_title,
                session.media_type,
                session.series_name,
                session.season_number,
                session.episode_number,
                session.started_at.isoformat(),
                session.ended_at.isoformat() if session.ended_at else None,
                session.play_duration_seconds,
                session.is_active,
                session.last_progress_update.isoformat(),
            ),
        )
        await self.conn.commit()
        return cursor.lastrowid

    async def get_active_session(self, session_id: str) -> Optional[Session]:
        """Get an active session by session ID."""
        cursor = await self.conn.execute(
            "SELECT * FROM sessions WHERE session_id = ? AND is_active = TRUE",
            (session_id,),
        )
        row = await cursor.fetchone()
        if row:
            return self._row_to_session(row)
        return None

    async def get_session_by_id(self, session_id: str) -> Optional[Session]:
        """Get any session by session ID (active or not)."""
        cursor = await self.conn.execute(
            "SELECT * FROM sessions WHERE session_id = ?",
            (session_id,),
        )
        row = await cursor.fetchone()
        if row:
            return self._row_to_session(row)
        return None

    async def update_session_progress(
        self, session_id: str, duration_seconds: int
    ) -> None:
        """Update session progress timestamp and duration."""
        now = datetime.now()
        await self.conn.execute(
            """
            UPDATE sessions
            SET last_progress_update = ?, play_duration_seconds = ?
            WHERE session_id = ? AND is_active = TRUE
            """,
            (now.isoformat(), duration_seconds, session_id),
        )
        await self.conn.commit()

    async def end_session(self, session_id: str, duration_seconds: int) -> None:
        """End a playback session."""
        now = datetime.now()
        await self.conn.execute(
            """
            UPDATE sessions
            SET ended_at = ?, is_active = FALSE, play_duration_seconds = ?
            WHERE session_id = ? AND is_active = TRUE
            """,
            (now.isoformat(), duration_seconds, session_id),
        )
        await self.conn.commit()

    async def timeout_stale_sessions(self, timeout_minutes: int) -> int:
        """End sessions that haven't received updates within timeout period."""
        cutoff = datetime.now() - timedelta(minutes=timeout_minutes)
        cursor = await self.conn.execute(
            """
            UPDATE sessions
            SET ended_at = last_progress_update, is_active = FALSE
            WHERE is_active = TRUE AND last_progress_update < ?
            """,
            (cutoff.isoformat(),),
        )
        await self.conn.commit()
        return cursor.rowcount

    async def get_active_sessions(self) -> list[Session]:
        """Get all active sessions."""
        cursor = await self.conn.execute(
            "SELECT * FROM sessions WHERE is_active = TRUE ORDER BY started_at DESC"
        )
        rows = await cursor.fetchall()
        return [self._row_to_session(row) for row in rows]

    async def get_user_watchtime(self, days: int = 30) -> list[UserWatchtime]:
        """Get watchtime statistics per user."""
        since = datetime.now() - timedelta(days=days)
        cursor = await self.conn.execute(
            """
            SELECT
                user_id,
                user_name,
                SUM(play_duration_seconds) as total_seconds,
                COUNT(*) as session_count
            FROM sessions
            WHERE started_at >= ?
            GROUP BY user_id, user_name
            ORDER BY total_seconds DESC
            """,
            (since.isoformat(),),
        )
        rows = await cursor.fetchall()
        return [
            UserWatchtime(
                user_id=row["user_id"],
                user_name=row["user_name"],
                total_seconds=row["total_seconds"] or 0,
                session_count=row["session_count"],
            )
            for row in rows
        ]

    async def get_top_media(self, days: int = 30, limit: int = 10) -> list[TopMedia]:
        """Get top watched media."""
        since = datetime.now() - timedelta(days=days)
        cursor = await self.conn.execute(
            """
            SELECT
                media_id,
                media_title,
                media_type,
                series_name,
                SUM(play_duration_seconds) as total_seconds,
                COUNT(*) as play_count
            FROM sessions
            WHERE started_at >= ?
            GROUP BY media_id, media_title, media_type, series_name
            ORDER BY total_seconds DESC
            LIMIT ?
            """,
            (since.isoformat(), limit),
        )
        rows = await cursor.fetchall()
        return [
            TopMedia(
                media_id=row["media_id"],
                media_title=row["media_title"],
                media_type=row["media_type"],
                series_name=row["series_name"],
                total_seconds=row["total_seconds"] or 0,
                play_count=row["play_count"],
            )
            for row in rows
        ]

    async def get_hourly_stats(self, days: int = 30) -> list[HourlyStats]:
        """Get usage statistics by hour of day."""
        since = datetime.now() - timedelta(days=days)
        cursor = await self.conn.execute(
            """
            SELECT
                CAST(strftime('%H', started_at) AS INTEGER) as hour,
                COUNT(*) as session_count,
                SUM(play_duration_seconds) as total_seconds
            FROM sessions
            WHERE started_at >= ?
            GROUP BY hour
            ORDER BY hour
            """,
            (since.isoformat(),),
        )
        rows = await cursor.fetchall()
        return [
            HourlyStats(
                hour=row["hour"],
                session_count=row["session_count"],
                total_seconds=row["total_seconds"] or 0,
            )
            for row in rows
        ]

    async def get_device_stats(self, days: int = 30) -> list[DeviceStats]:
        """Get device usage statistics."""
        since = datetime.now() - timedelta(days=days)
        cursor = await self.conn.execute(
            """
            SELECT
                device_name,
                client_name,
                COUNT(*) as session_count,
                SUM(play_duration_seconds) as total_seconds
            FROM sessions
            WHERE started_at >= ?
            GROUP BY device_name, client_name
            ORDER BY total_seconds DESC
            """,
            (since.isoformat(),),
        )
        rows = await cursor.fetchall()
        return [
            DeviceStats(
                device_name=row["device_name"],
                client_name=row["client_name"],
                session_count=row["session_count"],
                total_seconds=row["total_seconds"] or 0,
            )
            for row in rows
        ]

    async def get_daily_stats(self, days: int = 30) -> list[dict]:
        """Get daily usage statistics."""
        since = datetime.now() - timedelta(days=days)
        cursor = await self.conn.execute(
            """
            SELECT
                date(started_at) as date,
                COUNT(*) as session_count,
                SUM(play_duration_seconds) as total_seconds
            FROM sessions
            WHERE started_at >= ?
            GROUP BY date(started_at)
            ORDER BY date
            """,
            (since.isoformat(),),
        )
        rows = await cursor.fetchall()
        return [
            {
                "date": row["date"],
                "session_count": row["session_count"],
                "total_seconds": row["total_seconds"] or 0,
            }
            for row in rows
        ]

    async def get_summary_stats(self, days: int = 30) -> dict:
        """Get summary statistics."""
        since = datetime.now() - timedelta(days=days)
        cursor = await self.conn.execute(
            """
            SELECT
                COUNT(*) as total_sessions,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(DISTINCT media_id) as unique_media,
                COALESCE(SUM(play_duration_seconds), 0) as total_seconds
            FROM sessions
            WHERE started_at >= ?
            """,
            (since.isoformat(),),
        )
        row = await cursor.fetchone()
        return {
            "total_sessions": row["total_sessions"],
            "unique_users": row["unique_users"],
            "unique_media": row["unique_media"],
            "total_seconds": row["total_seconds"],
        }

    async def get_media_type_stats(self, days: int = 30) -> list[dict]:
        """Get statistics by media type."""
        since = datetime.now() - timedelta(days=days)
        cursor = await self.conn.execute(
            """
            SELECT
                media_type,
                COUNT(*) as session_count,
                SUM(play_duration_seconds) as total_seconds
            FROM sessions
            WHERE started_at >= ?
            GROUP BY media_type
            ORDER BY total_seconds DESC
            """,
            (since.isoformat(),),
        )
        rows = await cursor.fetchall()
        return [
            {
                "media_type": row["media_type"],
                "session_count": row["session_count"],
                "total_seconds": row["total_seconds"] or 0,
            }
            for row in rows
        ]

    async def get_recent_activity(self, limit: int = 20) -> list[Session]:
        """Get recent playback activity."""
        cursor = await self.conn.execute(
            """
            SELECT * FROM sessions
            WHERE is_active = FALSE
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = await cursor.fetchall()
        return [self._row_to_session(row) for row in rows]

    async def get_user_stats(self, user_id: str, days: int = 30) -> dict:
        """Get detailed statistics for a specific user."""
        since = datetime.now() - timedelta(days=days)

        # Basic stats
        cursor = await self.conn.execute(
            """
            SELECT
                user_name,
                COUNT(*) as total_sessions,
                COALESCE(SUM(play_duration_seconds), 0) as total_seconds,
                COUNT(DISTINCT media_id) as unique_media
            FROM sessions
            WHERE user_id = ? AND started_at >= ?
            """,
            (user_id, since.isoformat()),
        )
        row = await cursor.fetchone()
        basic = {
            "user_id": user_id,
            "user_name": row["user_name"] or "Unknown",
            "total_sessions": row["total_sessions"],
            "total_seconds": row["total_seconds"],
            "unique_media": row["unique_media"],
        }

        # Top media for user
        cursor = await self.conn.execute(
            """
            SELECT
                media_title,
                series_name,
                media_type,
                COUNT(*) as play_count,
                SUM(play_duration_seconds) as total_seconds
            FROM sessions
            WHERE user_id = ? AND started_at >= ?
            GROUP BY media_id, media_title, series_name, media_type
            ORDER BY total_seconds DESC
            LIMIT 10
            """,
            (user_id, since.isoformat()),
        )
        rows = await cursor.fetchall()
        top_media = [
            {
                "media_title": r["media_title"],
                "series_name": r["series_name"],
                "media_type": r["media_type"],
                "play_count": r["play_count"],
                "total_seconds": r["total_seconds"] or 0,
            }
            for r in rows
        ]

        # Recent activity for user
        cursor = await self.conn.execute(
            """
            SELECT * FROM sessions
            WHERE user_id = ?
            ORDER BY started_at DESC
            LIMIT 20
            """,
            (user_id,),
        )
        rows = await cursor.fetchall()
        recent = [self._row_to_session(row) for row in rows]

        return {**basic, "top_media": top_media, "recent_activity": recent}

    def _row_to_session(self, row: aiosqlite.Row) -> Session:
        """Convert a database row to a Session model."""
        return Session(
            id=row["id"],
            session_id=row["session_id"],
            user_id=row["user_id"],
            user_name=row["user_name"],
            device_id=row["device_id"],
            device_name=row["device_name"],
            client_name=row["client_name"],
            media_id=row["media_id"],
            media_title=row["media_title"],
            media_type=row["media_type"],
            series_name=row["series_name"],
            season_number=row["season_number"],
            episode_number=row["episode_number"],
            started_at=datetime.fromisoformat(row["started_at"]),
            ended_at=(
                datetime.fromisoformat(row["ended_at"]) if row["ended_at"] else None
            ),
            play_duration_seconds=row["play_duration_seconds"],
            is_active=bool(row["is_active"]),
            last_progress_update=datetime.fromisoformat(row["last_progress_update"]),
        )


# Global database instance
db = Database()
