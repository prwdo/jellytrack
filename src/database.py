from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import aiosqlite

from .config import settings
from .models import DeviceStats, HourlyStats, Session, TopMedia, UserWatchtime


class Database:
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or settings.database_path_resolved
        self._connection: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        """Connect to the database and create tables if needed."""
        self._connection = await aiosqlite.connect(self.db_path)
        self._connection.row_factory = aiosqlite.Row
        await self._create_tables()
        await self._ensure_columns()
        await self._create_aggregate_tables()

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

    def _include_aggregates(self, days: int) -> bool:
        return days > settings.retention_days

    def _build_filter_clause(
        self,
        user_id: Optional[str],
        device_name: Optional[str],
        media_type: Optional[str],
    ) -> tuple[str, list[str]]:
        clauses = []
        params: list[str] = []
        exclusion_clause, exclusion_params = self._build_exclusion_clause()
        if exclusion_clause:
            clauses.append(exclusion_clause)
            params.extend(exclusion_params)
        if user_id:
            clauses.append("user_id = ?")
            params.append(user_id)
        if device_name:
            clauses.append("device_name = ?")
            params.append(device_name)
        if media_type:
            clauses.append("media_type = ?")
            params.append(media_type)
        if not clauses:
            return "", []
        return " AND " + " AND ".join(clauses), params

    def _build_exclusion_clause(self) -> tuple[str, list[str]]:
        excluded = settings.excluded_user_names_list
        if not excluded:
            return "", []
        placeholders = ", ".join("?" for _ in excluded)
        return f"(user_name IS NULL OR user_name NOT IN ({placeholders}))", excluded

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
                paused_duration_seconds INTEGER DEFAULT 0,
                is_active BOOLEAN DEFAULT TRUE,
                last_position_seconds INTEGER DEFAULT 0,
                last_state_is_paused BOOLEAN DEFAULT FALSE,
                last_progress_update TIMESTAMP
            )
        """)
        await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)")
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sessions_active ON sessions(is_active)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sessions_started ON sessions(started_at)"
        )
        await self.conn.commit()

    async def _ensure_columns(self) -> None:
        """Add missing columns for backwards-compatible upgrades."""
        cursor = await self.conn.execute("PRAGMA table_info(sessions)")
        rows = await cursor.fetchall()
        existing = {row["name"] for row in rows}
        missing = {
            "paused_duration_seconds": "INTEGER DEFAULT 0",
            "last_position_seconds": "INTEGER DEFAULT 0",
            "last_state_is_paused": "BOOLEAN DEFAULT FALSE",
        }
        added = False
        for column, definition in missing.items():
            if column not in existing:
                await self.conn.execute(f"ALTER TABLE sessions ADD COLUMN {column} {definition}")
                added = True
        if added:
            await self.conn.commit()

    async def _create_aggregate_tables(self) -> None:
        """Create aggregation tables if they don't exist."""
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS session_aggregates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                hour INTEGER NOT NULL,
                user_id TEXT,
                user_name TEXT,
                media_id TEXT,
                media_title TEXT,
                media_type TEXT,
                series_name TEXT,
                device_name TEXT,
                client_name TEXT,
                session_count INTEGER DEFAULT 0,
                play_seconds INTEGER DEFAULT 0,
                paused_seconds INTEGER DEFAULT 0
            )
        """)
        await self.conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_aggregates_unique
            ON session_aggregates(date, hour, user_id, media_id, device_name, client_name)
            """
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_aggregates_date ON session_aggregates(date)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_aggregates_user ON session_aggregates(user_id)"
        )
        await self.conn.commit()

    async def create_session(self, session: Session) -> int:
        """Create or update a playback session (UPSERT)."""
        cursor = await self.conn.execute(
            """
            INSERT INTO sessions (session_id, user_id, user_name, device_id, device_name,
                                  client_name, media_id, media_title, media_type, series_name,
                                  season_number, episode_number, started_at, ended_at,
                                  play_duration_seconds, paused_duration_seconds, is_active,
                                  last_position_seconds, last_state_is_paused, last_progress_update)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                user_id = excluded.user_id,
                user_name = excluded.user_name,
                device_id = excluded.device_id,
                device_name = excluded.device_name,
                client_name = excluded.client_name,
                media_id = excluded.media_id,
                media_title = excluded.media_title,
                media_type = excluded.media_type,
                series_name = excluded.series_name,
                season_number = excluded.season_number,
                episode_number = excluded.episode_number,
                started_at = excluded.started_at,
                ended_at = excluded.ended_at,
                play_duration_seconds = excluded.play_duration_seconds,
                paused_duration_seconds = excluded.paused_duration_seconds,
                is_active = excluded.is_active,
                last_position_seconds = excluded.last_position_seconds,
                last_state_is_paused = excluded.last_state_is_paused,
                last_progress_update = excluded.last_progress_update
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
                session.paused_duration_seconds,
                session.is_active,
                session.last_position_seconds,
                session.last_state_is_paused,
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

    async def update_session_state(
        self,
        session_id: str,
        position_seconds: int,
        is_paused: bool,
        play_add_seconds: int,
        paused_add_seconds: int,
        now: datetime,
    ) -> None:
        """Update session state with deltas and latest position."""
        await self.conn.execute(
            """
            UPDATE sessions
            SET last_progress_update = ?,
                play_duration_seconds = play_duration_seconds + ?,
                paused_duration_seconds = paused_duration_seconds + ?,
                last_position_seconds = ?,
                last_state_is_paused = ?
            WHERE session_id = ? AND is_active = TRUE
            """,
            (
                now.isoformat(),
                play_add_seconds,
                paused_add_seconds,
                position_seconds,
                is_paused,
                session_id,
            ),
        )
        await self.conn.commit()

    async def end_session(self, session_id: str) -> None:
        """End a playback session."""
        now = datetime.now()
        await self.conn.execute(
            """
            UPDATE sessions
            SET ended_at = ?, is_active = FALSE
            WHERE session_id = ? AND is_active = TRUE
            """,
            (now.isoformat(), session_id),
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

    async def aggregate_and_prune(self, retention_days: int) -> int:
        """Aggregate and prune sessions older than retention_days."""
        cutoff = datetime.now() - timedelta(days=retention_days)
        cutoff_iso = cutoff.isoformat()
        await self.conn.execute("BEGIN")
        try:
            await self.conn.execute(
                """
                INSERT INTO session_aggregates (
                    date, hour, user_id, user_name, media_id, media_title, media_type,
                    series_name, device_name, client_name, session_count, play_seconds,
                    paused_seconds
                )
                SELECT
                    date(started_at) as date,
                    CAST(strftime('%H', started_at) AS INTEGER) as hour,
                    user_id,
                    MAX(user_name) as user_name,
                    media_id,
                    MAX(media_title) as media_title,
                    MAX(media_type) as media_type,
                    MAX(series_name) as series_name,
                    device_name,
                    client_name,
                    COUNT(*) as session_count,
                    SUM(play_duration_seconds) as play_seconds,
                    SUM(paused_duration_seconds) as paused_seconds
                FROM sessions
                WHERE started_at < ? AND is_active = FALSE
                GROUP BY date, hour, user_id, media_id, device_name, client_name
                ON CONFLICT(date, hour, user_id, media_id, device_name, client_name)
                DO UPDATE SET
                    session_count = session_count + excluded.session_count,
                    play_seconds = play_seconds + excluded.play_seconds,
                    paused_seconds = paused_seconds + excluded.paused_seconds,
                    user_name = excluded.user_name,
                    media_title = excluded.media_title,
                    media_type = excluded.media_type,
                    series_name = excluded.series_name
                """,
                (cutoff_iso,),
            )
            cursor = await self.conn.execute(
                """
                DELETE FROM sessions
                WHERE started_at < ? AND is_active = FALSE
                """,
                (cutoff_iso,),
            )
            await self.conn.commit()
            return cursor.rowcount
        except Exception:
            await self.conn.execute("ROLLBACK")
            raise

    async def get_active_sessions(
        self,
        user_id: Optional[str] = None,
        device_name: Optional[str] = None,
        media_type: Optional[str] = None,
    ) -> list[Session]:
        """Get all active sessions."""
        filters, params = self._build_filter_clause(user_id, device_name, media_type)
        cursor = await self.conn.execute(
            f"""
            SELECT * FROM sessions
            WHERE is_active = TRUE{filters}
            ORDER BY started_at DESC
            """,
            params,
        )
        rows = await cursor.fetchall()
        return [self._row_to_session(row) for row in rows]

    async def get_user_watchtime(
        self,
        days: int = 30,
        user_id: Optional[str] = None,
        device_name: Optional[str] = None,
        media_type: Optional[str] = None,
    ) -> list[UserWatchtime]:
        """Get watchtime statistics per user."""
        since = datetime.now() - timedelta(days=days)
        filters, params = self._build_filter_clause(user_id, device_name, media_type)
        if self._include_aggregates(days):
            cursor = await self.conn.execute(
                f"""
                SELECT
                    user_id,
                    user_name,
                    SUM(total_seconds) as total_seconds,
                    SUM(session_count) as session_count
                FROM (
                    SELECT
                        user_id,
                        user_name,
                        SUM(play_duration_seconds) as total_seconds,
                        COUNT(*) as session_count
                    FROM sessions
                    WHERE started_at >= ?{filters}
                    GROUP BY user_id, user_name
                    UNION ALL
                    SELECT
                        user_id,
                        user_name,
                        SUM(play_seconds) as total_seconds,
                        SUM(session_count) as session_count
                    FROM session_aggregates
                    WHERE date >= ?{filters}
                    GROUP BY user_id, user_name
                )
                GROUP BY user_id, user_name
                ORDER BY total_seconds DESC
                """,
                (since.isoformat(), *params, since.date().isoformat(), *params),
            )
        else:
            cursor = await self.conn.execute(
                f"""
                SELECT
                    user_id,
                    user_name,
                    SUM(play_duration_seconds) as total_seconds,
                    COUNT(*) as session_count
                FROM sessions
                WHERE started_at >= ?{filters}
                GROUP BY user_id, user_name
                ORDER BY total_seconds DESC
                """,
                (since.isoformat(), *params),
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

    async def get_top_media(
        self,
        days: int = 30,
        limit: int = 10,
        user_id: Optional[str] = None,
        device_name: Optional[str] = None,
        media_type: Optional[str] = None,
    ) -> list[TopMedia]:
        """Get top watched media."""
        since = datetime.now() - timedelta(days=days)
        filters, params = self._build_filter_clause(user_id, device_name, media_type)
        if self._include_aggregates(days):
            cursor = await self.conn.execute(
                f"""
                WITH base AS (
                    SELECT
                        CASE
                            WHEN media_type = 'Episode' AND series_name IS NOT NULL
                                THEN series_name
                            ELSE media_id
                        END as media_id,
                        CASE
                            WHEN media_type = 'Episode' AND series_name IS NOT NULL
                                THEN series_name
                            ELSE media_title
                        END as media_title,
                        media_type,
                        series_name,
                        play_duration_seconds as total_seconds,
                        1 as play_count
                    FROM sessions
                    WHERE started_at >= ?{filters}
                    UNION ALL
                    SELECT
                        CASE
                            WHEN media_type = 'Episode' AND series_name IS NOT NULL
                                THEN series_name
                            ELSE media_id
                        END as media_id,
                        CASE
                            WHEN media_type = 'Episode' AND series_name IS NOT NULL
                                THEN series_name
                            ELSE media_title
                        END as media_title,
                        media_type,
                        series_name,
                        play_seconds as total_seconds,
                        session_count as play_count
                    FROM session_aggregates
                    WHERE date >= ?{filters}
                )
                SELECT
                    media_id,
                    media_title,
                    media_type,
                    series_name,
                    SUM(total_seconds) as total_seconds,
                    SUM(play_count) as play_count
                FROM base
                GROUP BY media_id, media_title, media_type, series_name
                ORDER BY total_seconds DESC
                LIMIT ?
                """,
                (since.isoformat(), *params, since.date().isoformat(), *params, limit),
            )
        else:
            cursor = await self.conn.execute(
                f"""
                WITH base AS (
                    SELECT
                        CASE
                            WHEN media_type = 'Episode' AND series_name IS NOT NULL
                                THEN series_name
                            ELSE media_id
                        END as media_id,
                        CASE
                            WHEN media_type = 'Episode' AND series_name IS NOT NULL
                                THEN series_name
                            ELSE media_title
                        END as media_title,
                        media_type,
                        series_name,
                        play_duration_seconds as total_seconds,
                        1 as play_count
                    FROM sessions
                    WHERE started_at >= ?{filters}
                )
                SELECT
                    media_id,
                    media_title,
                    media_type,
                    series_name,
                    SUM(total_seconds) as total_seconds,
                    SUM(play_count) as play_count
                FROM base
                GROUP BY media_id, media_title, media_type, series_name
                ORDER BY total_seconds DESC
                LIMIT ?
                """,
                (since.isoformat(), *params, limit),
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

    async def get_hourly_stats(
        self,
        days: int = 30,
        user_id: Optional[str] = None,
        device_name: Optional[str] = None,
        media_type: Optional[str] = None,
    ) -> list[HourlyStats]:
        """Get usage statistics by hour of day."""
        since = datetime.now() - timedelta(days=days)
        filters, params = self._build_filter_clause(user_id, device_name, media_type)
        if self._include_aggregates(days):
            cursor = await self.conn.execute(
                f"""
                SELECT
                    hour,
                    SUM(session_count) as session_count,
                    SUM(total_seconds) as total_seconds
                FROM (
                    SELECT
                        CAST(strftime('%H', started_at) AS INTEGER) as hour,
                        COUNT(*) as session_count,
                        SUM(play_duration_seconds) as total_seconds
                    FROM sessions
                    WHERE started_at >= ?{filters}
                    GROUP BY hour
                    UNION ALL
                    SELECT
                        hour,
                        SUM(session_count) as session_count,
                        SUM(play_seconds) as total_seconds
                    FROM session_aggregates
                    WHERE date >= ?{filters}
                    GROUP BY hour
                )
                GROUP BY hour
                ORDER BY hour
                """,
                (since.isoformat(), *params, since.date().isoformat(), *params),
            )
        else:
            cursor = await self.conn.execute(
                f"""
                SELECT
                    CAST(strftime('%H', started_at) AS INTEGER) as hour,
                    COUNT(*) as session_count,
                    SUM(play_duration_seconds) as total_seconds
                FROM sessions
                WHERE started_at >= ?{filters}
                GROUP BY hour
                ORDER BY hour
                """,
                (since.isoformat(), *params),
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

    async def get_device_stats(
        self,
        days: int = 30,
        user_id: Optional[str] = None,
        device_name: Optional[str] = None,
        media_type: Optional[str] = None,
    ) -> list[DeviceStats]:
        """Get device usage statistics."""
        since = datetime.now() - timedelta(days=days)
        filters, params = self._build_filter_clause(user_id, device_name, media_type)
        if self._include_aggregates(days):
            cursor = await self.conn.execute(
                f"""
                SELECT
                    device_name,
                    client_name,
                    SUM(session_count) as session_count,
                    SUM(total_seconds) as total_seconds
                FROM (
                    SELECT
                        device_name,
                        client_name,
                        COUNT(*) as session_count,
                        SUM(play_duration_seconds) as total_seconds
                    FROM sessions
                    WHERE started_at >= ?{filters}
                    GROUP BY device_name, client_name
                    UNION ALL
                    SELECT
                        device_name,
                        client_name,
                        SUM(session_count) as session_count,
                        SUM(play_seconds) as total_seconds
                    FROM session_aggregates
                    WHERE date >= ?{filters}
                    GROUP BY device_name, client_name
                )
                GROUP BY device_name, client_name
                ORDER BY total_seconds DESC
                """,
                (since.isoformat(), *params, since.date().isoformat(), *params),
            )
        else:
            cursor = await self.conn.execute(
                f"""
                SELECT
                    device_name,
                    client_name,
                    COUNT(*) as session_count,
                    SUM(play_duration_seconds) as total_seconds
                FROM sessions
                WHERE started_at >= ?{filters}
                GROUP BY device_name, client_name
                ORDER BY total_seconds DESC
                """,
                (since.isoformat(), *params),
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

    async def get_pause_ratio_by_device(
        self,
        days: int = 30,
        user_id: Optional[str] = None,
        device_name: Optional[str] = None,
        media_type: Optional[str] = None,
    ) -> list[dict]:
        """Get play vs pause totals per device/client."""
        since = datetime.now() - timedelta(days=days)
        filters, params = self._build_filter_clause(user_id, device_name, media_type)
        if self._include_aggregates(days):
            cursor = await self.conn.execute(
                f"""
                SELECT
                    device_name,
                    client_name,
                    SUM(play_seconds) as play_seconds,
                    SUM(paused_seconds) as paused_seconds,
                    SUM(session_count) as session_count
                FROM (
                    SELECT
                        device_name,
                        client_name,
                        COALESCE(SUM(play_duration_seconds), 0) as play_seconds,
                        COALESCE(SUM(paused_duration_seconds), 0) as paused_seconds,
                        COUNT(*) as session_count
                    FROM sessions
                    WHERE started_at >= ?{filters}
                    GROUP BY device_name, client_name
                    UNION ALL
                    SELECT
                        device_name,
                        client_name,
                        COALESCE(SUM(play_seconds), 0) as play_seconds,
                        COALESCE(SUM(paused_seconds), 0) as paused_seconds,
                        COALESCE(SUM(session_count), 0) as session_count
                    FROM session_aggregates
                    WHERE date >= ?{filters}
                    GROUP BY device_name, client_name
                )
                GROUP BY device_name, client_name
                ORDER BY (play_seconds + paused_seconds) DESC
                """,
                (since.isoformat(), *params, since.date().isoformat(), *params),
            )
        else:
            cursor = await self.conn.execute(
                f"""
                SELECT
                    device_name,
                    client_name,
                    COALESCE(SUM(play_duration_seconds), 0) as play_seconds,
                    COALESCE(SUM(paused_duration_seconds), 0) as paused_seconds,
                    COUNT(*) as session_count
                FROM sessions
                WHERE started_at >= ?{filters}
                GROUP BY device_name, client_name
                ORDER BY (play_seconds + paused_seconds) DESC
                """,
                (since.isoformat(), *params),
            )
        rows = await cursor.fetchall()
        return [
            {
                "device_name": row["device_name"],
                "client_name": row["client_name"],
                "play_seconds": row["play_seconds"] or 0,
                "paused_seconds": row["paused_seconds"] or 0,
                "session_count": row["session_count"] or 0,
            }
            for row in rows
        ]

    async def get_hourly_weekday_heatmap(
        self,
        days: int = 30,
        user_id: Optional[str] = None,
        device_name: Optional[str] = None,
        media_type: Optional[str] = None,
    ) -> list[dict]:
        """Get watchtime per weekday/hour (seconds)."""
        since = datetime.now() - timedelta(days=days)
        filters, params = self._build_filter_clause(user_id, device_name, media_type)
        if self._include_aggregates(days):
            cursor = await self.conn.execute(
                f"""
                SELECT
                    weekday,
                    hour,
                    SUM(play_seconds) as watch_seconds
                FROM (
                    SELECT
                        CAST(strftime('%w', started_at) AS INTEGER) as weekday,
                        CAST(strftime('%H', started_at) AS INTEGER) as hour,
                        SUM(play_duration_seconds) as play_seconds
                    FROM sessions
                    WHERE started_at >= ?{filters}
                    GROUP BY weekday, hour
                    UNION ALL
                    SELECT
                        CAST(strftime('%w', date) AS INTEGER) as weekday,
                        hour,
                        SUM(play_seconds) as play_seconds
                    FROM session_aggregates
                    WHERE date >= ?{filters}
                    GROUP BY weekday, hour
                )
                GROUP BY weekday, hour
                ORDER BY weekday, hour
                """,
                (since.isoformat(), *params, since.date().isoformat(), *params),
            )
        else:
            cursor = await self.conn.execute(
                f"""
                SELECT
                    CAST(strftime('%w', started_at) AS INTEGER) as weekday,
                    CAST(strftime('%H', started_at) AS INTEGER) as hour,
                    SUM(play_duration_seconds) as watch_seconds
                FROM sessions
                WHERE started_at >= ?{filters}
                GROUP BY weekday, hour
                ORDER BY weekday, hour
                """,
                (since.isoformat(), *params),
            )
        rows = await cursor.fetchall()
        return [
            {
                "weekday": row["weekday"],
                "hour": row["hour"],
                "watch_seconds": row["watch_seconds"] or 0,
            }
            for row in rows
        ]

    async def get_series_daily_totals(
        self,
        days: int = 30,
        user_id: Optional[str] = None,
        device_name: Optional[str] = None,
        media_type: Optional[str] = None,
    ) -> list[dict]:
        """Get daily totals per series."""
        since = datetime.now() - timedelta(days=days)
        filters, params = self._build_filter_clause(user_id, device_name, media_type)
        if self._include_aggregates(days):
            cursor = await self.conn.execute(
                f"""
                SELECT
                    date,
                    series_name,
                    SUM(total_seconds) as total_seconds
                FROM (
                    SELECT
                        date(started_at) as date,
                        series_name,
                        SUM(play_duration_seconds) as total_seconds
                    FROM sessions
                    WHERE started_at >= ?{filters} AND series_name IS NOT NULL
                    GROUP BY date, series_name
                    UNION ALL
                    SELECT
                        date,
                        series_name,
                        SUM(play_seconds) as total_seconds
                    FROM session_aggregates
                    WHERE date >= ?{filters} AND series_name IS NOT NULL
                    GROUP BY date, series_name
                )
                GROUP BY date, series_name
                ORDER BY date
                """,
                (since.isoformat(), *params, since.date().isoformat(), *params),
            )
        else:
            cursor = await self.conn.execute(
                f"""
                SELECT
                    date(started_at) as date,
                    series_name,
                    SUM(play_duration_seconds) as total_seconds
                FROM sessions
                WHERE started_at >= ?{filters} AND series_name IS NOT NULL
                GROUP BY date, series_name
                ORDER BY date
                """,
                (since.isoformat(), *params),
            )
        rows = await cursor.fetchall()
        return [
            {
                "date": row["date"],
                "series_name": row["series_name"],
                "total_seconds": row["total_seconds"] or 0,
            }
            for row in rows
        ]

    async def get_sessions_for_metrics(
        self,
        days: int = 30,
        user_id: Optional[str] = None,
        device_name: Optional[str] = None,
        media_type: Optional[str] = None,
    ) -> list[dict]:
        """Get sessions for derived metrics (distribution/concurrency/completion)."""
        since = datetime.now() - timedelta(days=days)
        filters, params = self._build_filter_clause(user_id, device_name, media_type)
        cursor = await self.conn.execute(
            f"""
            SELECT
                session_id,
                media_id,
                media_type,
                started_at,
                ended_at,
                is_active,
                play_duration_seconds,
                paused_duration_seconds,
                last_position_seconds,
                last_progress_update
            FROM sessions
            WHERE (started_at >= ? OR ended_at >= ? OR is_active = TRUE){filters}
            """,
            (since.isoformat(), since.isoformat(), *params),
        )
        rows = await cursor.fetchall()
        return [
            {
                "session_id": row["session_id"],
                "media_id": row["media_id"],
                "media_type": row["media_type"],
                "started_at": row["started_at"],
                "ended_at": row["ended_at"],
                "is_active": bool(row["is_active"]),
                "play_seconds": row["play_duration_seconds"] or 0,
                "paused_seconds": row["paused_duration_seconds"] or 0,
                "last_position_seconds": row["last_position_seconds"] or 0,
                "last_progress_update": row["last_progress_update"],
            }
            for row in rows
        ]

    async def get_daily_stats(
        self,
        days: int = 30,
        user_id: Optional[str] = None,
        device_name: Optional[str] = None,
        media_type: Optional[str] = None,
    ) -> list[dict]:
        """Get daily usage statistics."""
        since = datetime.now() - timedelta(days=days)
        filters, params = self._build_filter_clause(user_id, device_name, media_type)
        if self._include_aggregates(days):
            cursor = await self.conn.execute(
                f"""
                SELECT
                    date,
                    SUM(session_count) as session_count,
                    SUM(total_seconds) as total_seconds
                FROM (
                    SELECT
                        date(started_at) as date,
                        COUNT(*) as session_count,
                        SUM(play_duration_seconds) as total_seconds
                    FROM sessions
                    WHERE started_at >= ?{filters}
                    GROUP BY date(started_at)
                    UNION ALL
                    SELECT
                        date,
                        SUM(session_count) as session_count,
                        SUM(play_seconds) as total_seconds
                    FROM session_aggregates
                    WHERE date >= ?{filters}
                    GROUP BY date
                )
                GROUP BY date
                ORDER BY date
                """,
                (since.isoformat(), *params, since.date().isoformat(), *params),
            )
        else:
            cursor = await self.conn.execute(
                f"""
                SELECT
                    date(started_at) as date,
                    COUNT(*) as session_count,
                    SUM(play_duration_seconds) as total_seconds
                FROM sessions
                WHERE started_at >= ?{filters}
                GROUP BY date(started_at)
                ORDER BY date
                """,
                (since.isoformat(), *params),
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

    async def get_summary_stats(
        self,
        days: int = 30,
        user_id: Optional[str] = None,
        device_name: Optional[str] = None,
        media_type: Optional[str] = None,
    ) -> dict:
        """Get summary statistics."""
        since = datetime.now() - timedelta(days=days)
        filters, params = self._build_filter_clause(user_id, device_name, media_type)
        if self._include_aggregates(days):
            cursor = await self.conn.execute(
                f"""
                SELECT
                    SUM(total_sessions) as total_sessions,
                    SUM(total_seconds) as total_seconds
                FROM (
                    SELECT
                        COUNT(*) as total_sessions,
                        COALESCE(SUM(play_duration_seconds), 0) as total_seconds
                    FROM sessions
                    WHERE started_at >= ?{filters}
                    UNION ALL
                    SELECT
                        COALESCE(SUM(session_count), 0) as total_sessions,
                        COALESCE(SUM(play_seconds), 0) as total_seconds
                    FROM session_aggregates
                    WHERE date >= ?{filters}
                )
                """,
                (since.isoformat(), *params, since.date().isoformat(), *params),
            )
            row = await cursor.fetchone()
            users_cursor = await self.conn.execute(
                f"""
                SELECT COUNT(DISTINCT user_id) as unique_users
                FROM (
                    SELECT user_id FROM sessions WHERE started_at >= ?{filters}
                    UNION
                    SELECT user_id FROM session_aggregates WHERE date >= ?{filters}
                )
                """,
                (since.isoformat(), *params, since.date().isoformat(), *params),
            )
            users_row = await users_cursor.fetchone()
            media_cursor = await self.conn.execute(
                f"""
                SELECT COUNT(DISTINCT media_id) as unique_media
                FROM (
                    SELECT media_id FROM sessions WHERE started_at >= ?{filters}
                    UNION
                    SELECT media_id FROM session_aggregates WHERE date >= ?{filters}
                )
                """,
                (since.isoformat(), *params, since.date().isoformat(), *params),
            )
            media_row = await media_cursor.fetchone()
            return {
                "total_sessions": row["total_sessions"] or 0,
                "unique_users": users_row["unique_users"] or 0,
                "unique_media": media_row["unique_media"] or 0,
                "total_seconds": row["total_seconds"] or 0,
            }
        cursor = await self.conn.execute(
            f"""
            SELECT
                COUNT(*) as total_sessions,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(DISTINCT media_id) as unique_media,
                COALESCE(SUM(play_duration_seconds), 0) as total_seconds
            FROM sessions
            WHERE started_at >= ?{filters}
            """,
            (since.isoformat(), *params),
        )
        row = await cursor.fetchone()
        return {
            "total_sessions": row["total_sessions"],
            "unique_users": row["unique_users"],
            "unique_media": row["unique_media"],
            "total_seconds": row["total_seconds"],
        }

    async def get_media_type_stats(
        self,
        days: int = 30,
        user_id: Optional[str] = None,
        device_name: Optional[str] = None,
        media_type: Optional[str] = None,
    ) -> list[dict]:
        """Get statistics by media type."""
        since = datetime.now() - timedelta(days=days)
        filters, params = self._build_filter_clause(user_id, device_name, media_type)
        if self._include_aggregates(days):
            cursor = await self.conn.execute(
                f"""
                SELECT
                    media_type,
                    SUM(session_count) as session_count,
                    SUM(total_seconds) as total_seconds
                FROM (
                    SELECT
                        media_type,
                        COUNT(*) as session_count,
                        SUM(play_duration_seconds) as total_seconds
                    FROM sessions
                    WHERE started_at >= ?{filters}
                    GROUP BY media_type
                    UNION ALL
                    SELECT
                        media_type,
                        SUM(session_count) as session_count,
                        SUM(play_seconds) as total_seconds
                    FROM session_aggregates
                    WHERE date >= ?{filters}
                    GROUP BY media_type
                )
                GROUP BY media_type
                ORDER BY total_seconds DESC
                """,
                (since.isoformat(), *params, since.date().isoformat(), *params),
            )
        else:
            cursor = await self.conn.execute(
                f"""
                SELECT
                    media_type,
                    COUNT(*) as session_count,
                    SUM(play_duration_seconds) as total_seconds
                FROM sessions
                WHERE started_at >= ?{filters}
                GROUP BY media_type
                ORDER BY total_seconds DESC
                """,
                (since.isoformat(), *params),
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

    async def get_pause_stats(
        self,
        days: int = 30,
        user_id: Optional[str] = None,
        device_name: Optional[str] = None,
        media_type: Optional[str] = None,
    ) -> dict:
        """Get play vs pause totals."""
        since = datetime.now() - timedelta(days=days)
        filters, params = self._build_filter_clause(user_id, device_name, media_type)
        if self._include_aggregates(days):
            cursor = await self.conn.execute(
                f"""
                SELECT
                    SUM(play_seconds) as play_seconds,
                    SUM(paused_seconds) as paused_seconds
                FROM (
                    SELECT
                        COALESCE(SUM(play_duration_seconds), 0) as play_seconds,
                        COALESCE(SUM(paused_duration_seconds), 0) as paused_seconds
                    FROM sessions
                    WHERE started_at >= ?{filters}
                    UNION ALL
                    SELECT
                        COALESCE(SUM(play_seconds), 0) as play_seconds,
                        COALESCE(SUM(paused_seconds), 0) as paused_seconds
                    FROM session_aggregates
                    WHERE date >= ?{filters}
                )
                """,
                (since.isoformat(), *params, since.date().isoformat(), *params),
            )
        else:
            cursor = await self.conn.execute(
                f"""
                SELECT
                    COALESCE(SUM(play_duration_seconds), 0) as play_seconds,
                    COALESCE(SUM(paused_duration_seconds), 0) as paused_seconds
                FROM sessions
                WHERE started_at >= ?{filters}
                """,
                (since.isoformat(), *params),
            )
        row = await cursor.fetchone()
        return {
            "play_seconds": row["play_seconds"] or 0,
            "paused_seconds": row["paused_seconds"] or 0,
        }

    async def get_filter_options(self, days: int = 30) -> dict:
        """Get filter options for users, devices, and media types."""
        since = datetime.now() - timedelta(days=days)
        exclusion_clause, exclusion_params = self._build_exclusion_clause()
        user_filters = f" AND {exclusion_clause}" if exclusion_clause else ""
        if self._include_aggregates(days):
            users_cursor = await self.conn.execute(
                f"""
                SELECT user_id, user_name
                FROM (
                    SELECT user_id, user_name
                    FROM sessions
                    WHERE started_at >= ?{user_filters}
                    GROUP BY user_id, user_name
                    UNION
                    SELECT user_id, user_name
                    FROM session_aggregates
                    WHERE date >= ?{user_filters}
                    GROUP BY user_id, user_name
                )
                ORDER BY user_name
                """,
                (since.isoformat(), *exclusion_params, since.date().isoformat(), *exclusion_params),
            )
            devices_cursor = await self.conn.execute(
                """
                SELECT device_name
                FROM (
                    SELECT device_name
                    FROM sessions
                    WHERE started_at >= ?
                    GROUP BY device_name
                    UNION
                    SELECT device_name
                    FROM session_aggregates
                    WHERE date >= ?
                    GROUP BY device_name
                )
                ORDER BY device_name
                """,
                (since.isoformat(), since.date().isoformat()),
            )
            types_cursor = await self.conn.execute(
                """
                SELECT media_type
                FROM (
                    SELECT media_type
                    FROM sessions
                    WHERE started_at >= ?
                    GROUP BY media_type
                    UNION
                    SELECT media_type
                    FROM session_aggregates
                    WHERE date >= ?
                    GROUP BY media_type
                )
                ORDER BY media_type
                """,
                (since.isoformat(), since.date().isoformat()),
            )
        else:
            users_cursor = await self.conn.execute(
                f"""
                SELECT user_id, user_name
                FROM sessions
                WHERE started_at >= ?{user_filters}
                GROUP BY user_id, user_name
                ORDER BY user_name
                """,
                (since.isoformat(), *exclusion_params),
            )
            devices_cursor = await self.conn.execute(
                """
                SELECT device_name
                FROM sessions
                WHERE started_at >= ?
                GROUP BY device_name
                ORDER BY device_name
                """,
                (since.isoformat(),),
            )
            types_cursor = await self.conn.execute(
                """
                SELECT media_type
                FROM sessions
                WHERE started_at >= ?
                GROUP BY media_type
                ORDER BY media_type
                """,
                (since.isoformat(),),
            )
        users = await users_cursor.fetchall()
        devices = await devices_cursor.fetchall()
        types = await types_cursor.fetchall()
        return {
            "users": [{"id": row["user_id"], "name": row["user_name"]} for row in users],
            "devices": [row["device_name"] for row in devices],
            "media_types": [row["media_type"] for row in types],
        }

    async def get_recent_activity(
        self,
        limit: int = 20,
        user_id: Optional[str] = None,
        device_name: Optional[str] = None,
        media_type: Optional[str] = None,
    ) -> list[Session]:
        """Get recent playback activity."""
        filters, params = self._build_filter_clause(user_id, device_name, media_type)
        cursor = await self.conn.execute(
            f"""
            SELECT * FROM sessions
            WHERE is_active = FALSE{filters}
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (*params, limit),
        )
        rows = await cursor.fetchall()
        return [self._row_to_session(row) for row in rows]

    async def get_user_stats(self, user_id: str, days: int = 30) -> dict:
        """Get detailed statistics for a specific user."""
        since = datetime.now() - timedelta(days=days)

        if self._include_aggregates(days):
            cursor = await self.conn.execute(
                """
                SELECT
                    SUM(total_sessions) as total_sessions,
                    SUM(total_seconds) as total_seconds
                FROM (
                    SELECT
                        COUNT(*) as total_sessions,
                        COALESCE(SUM(play_duration_seconds), 0) as total_seconds
                    FROM sessions
                    WHERE user_id = ? AND started_at >= ?
                    UNION ALL
                    SELECT
                        COALESCE(SUM(session_count), 0) as total_sessions,
                        COALESCE(SUM(play_seconds), 0) as total_seconds
                    FROM session_aggregates
                    WHERE user_id = ? AND date >= ?
                )
                """,
                (user_id, since.isoformat(), user_id, since.date().isoformat()),
            )
            row = await cursor.fetchone()
            name_cursor = await self.conn.execute(
                """
                SELECT user_name
                FROM sessions
                WHERE user_id = ? AND started_at >= ?
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (user_id, since.isoformat()),
            )
            name_row = await name_cursor.fetchone()
            if not name_row:
                name_cursor = await self.conn.execute(
                    """
                    SELECT user_name
                    FROM session_aggregates
                    WHERE user_id = ? AND date >= ?
                    ORDER BY date DESC
                    LIMIT 1
                    """,
                    (user_id, since.date().isoformat()),
                )
                name_row = await name_cursor.fetchone()
            media_cursor = await self.conn.execute(
                """
                SELECT COUNT(DISTINCT media_id) as unique_media
                FROM (
                    SELECT media_id FROM sessions WHERE user_id = ? AND started_at >= ?
                    UNION
                    SELECT media_id FROM session_aggregates WHERE user_id = ? AND date >= ?
                )
                """,
                (user_id, since.isoformat(), user_id, since.date().isoformat()),
            )
            media_row = await media_cursor.fetchone()
            basic = {
                "user_id": user_id,
                "user_name": (name_row["user_name"] if name_row else "Unknown"),
                "total_sessions": row["total_sessions"] or 0,
                "total_seconds": row["total_seconds"] or 0,
                "unique_media": media_row["unique_media"] or 0,
            }
        else:
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

        if self._include_aggregates(days):
            cursor = await self.conn.execute(
                """
                SELECT
                    media_title,
                    series_name,
                    media_type,
                    SUM(play_count) as play_count,
                    SUM(total_seconds) as total_seconds
                FROM (
                    SELECT
                        media_title,
                        series_name,
                        media_type,
                        COUNT(*) as play_count,
                        SUM(play_duration_seconds) as total_seconds
                    FROM sessions
                    WHERE user_id = ? AND started_at >= ?
                    GROUP BY media_id, media_title, series_name, media_type
                    UNION ALL
                    SELECT
                        media_title,
                        series_name,
                        media_type,
                        SUM(session_count) as play_count,
                        SUM(play_seconds) as total_seconds
                    FROM session_aggregates
                    WHERE user_id = ? AND date >= ?
                    GROUP BY media_id, media_title, series_name, media_type
                )
                GROUP BY media_title, series_name, media_type
                ORDER BY total_seconds DESC
                LIMIT 10
                """,
                (user_id, since.isoformat(), user_id, since.date().isoformat()),
            )
        else:
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

        def parse_dt(value: Optional[str], fallback: Optional[datetime]) -> datetime:
            if value:
                return datetime.fromisoformat(value)
            if fallback:
                return fallback
            return datetime.fromtimestamp(0)

        started_at = parse_dt(row["started_at"], None)
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
            started_at=started_at,
            ended_at=(datetime.fromisoformat(row["ended_at"]) if row["ended_at"] else None),
            play_duration_seconds=row["play_duration_seconds"] or 0,
            paused_duration_seconds=row["paused_duration_seconds"] or 0,
            is_active=bool(row["is_active"]),
            last_position_seconds=row["last_position_seconds"] or 0,
            last_state_is_paused=bool(row["last_state_is_paused"]),
            last_progress_update=parse_dt(row["last_progress_update"], started_at),
        )


# Global database instance
db = Database()
