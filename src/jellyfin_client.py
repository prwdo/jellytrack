import asyncio
import json
import logging
from datetime import datetime
from typing import Optional, Callable, Awaitable

import websockets
from websockets.exceptions import ConnectionClosed

from .config import settings
from .database import db
from .models import Session, PlaybackEvent

logger = logging.getLogger(__name__)


class JellyfinWebSocketClient:
    def __init__(self):
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._reconnect_delay = 1
        self._max_reconnect_delay = 60
        self._session_durations: dict[str, int] = {}
        self._on_session_update: Optional[Callable[[], Awaitable[None]]] = None

    def set_session_update_callback(
        self, callback: Callable[[], Awaitable[None]]
    ) -> None:
        """Set callback to be called when sessions are updated."""
        self._on_session_update = callback

    async def start(self) -> None:
        """Start the WebSocket client with auto-reconnect."""
        self._running = True
        while self._running:
            try:
                await self._connect()
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                if self._running:
                    await asyncio.sleep(self._reconnect_delay)
                    self._reconnect_delay = min(
                        self._reconnect_delay * 2, self._max_reconnect_delay
                    )

    async def stop(self) -> None:
        """Stop the WebSocket client."""
        self._running = False
        if self.ws:
            await self.ws.close()

    async def _connect(self) -> None:
        """Connect to Jellyfin WebSocket and listen for events."""
        ws_url = settings.jellyfin_ws_url
        logger.info(f"Connecting to Jellyfin WebSocket: {ws_url.split('?')[0]}...")

        async with websockets.connect(ws_url) as ws:
            self.ws = ws
            self._reconnect_delay = 1
            logger.info("Connected to Jellyfin WebSocket")

            # Subscribe to session updates (every 2 seconds)
            await ws.send(json.dumps({
                "MessageType": "SessionsStart",
                "Data": "0,2000"
            }))
            logger.info("Subscribed to session updates")

            # Start timeout checker
            timeout_task = asyncio.create_task(self._check_timeouts())

            try:
                async for message in ws:
                    await self._handle_message(message)
            finally:
                timeout_task.cancel()
                try:
                    await timeout_task
                except asyncio.CancelledError:
                    pass

    async def _check_timeouts(self) -> None:
        """Periodically check for timed out sessions."""
        while True:
            await asyncio.sleep(60)
            try:
                count = await db.timeout_stale_sessions(
                    settings.session_timeout_minutes
                )
                if count > 0:
                    logger.info(f"Timed out {count} stale session(s)")
                    if self._on_session_update:
                        await self._on_session_update()
            except Exception as e:
                logger.error(f"Error checking timeouts: {e}")

    async def _handle_message(self, message: str) -> None:
        """Handle incoming WebSocket message."""
        try:
            data = json.loads(message)
            message_type = data.get("MessageType", "")

            if message_type == "Sessions":
                await self._handle_sessions(data.get("Data", []))
            elif message_type == "PlaybackStart":
                await self._handle_playback_start(data.get("Data", {}))
            elif message_type == "PlaybackStopped":
                await self._handle_playback_stop(data.get("Data", {}))
            elif message_type == "PlaybackProgress":
                # Jellyfin doesn't send dedicated progress messages over WS
                # Progress is included in Sessions updates
                pass

        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON message: {message[:100]}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")

    async def _handle_sessions(self, sessions: list[dict]) -> None:
        """Handle Sessions update message - track active playback."""
        active_session_ids = set()

        for session_data in sessions:
            now_playing = session_data.get("NowPlayingItem")
            if not now_playing:
                continue

            session_id = session_data.get("Id", "")
            if not session_id:
                continue
            active_session_ids.add(session_id)

            play_state = session_data.get("PlayState", {})
            position_ticks = play_state.get("PositionTicks", 0)
            is_paused = play_state.get("IsPaused", False)

            # Calculate duration in seconds
            duration_seconds = position_ticks // 10_000_000

            # Check if this is a new session
            existing = await db.get_active_session(session_id)
            if not existing:
                event = self._extract_playback_event(session_data, now_playing)
                await self._create_session(event)
            else:
                # Update progress and keep session fresh, even if paused.
                self._session_durations[session_id] = duration_seconds
                await db.update_session_progress(session_id, duration_seconds)

        # Check for ended sessions (not in active list anymore)
        active_db_sessions = await db.get_active_sessions()
        for db_session in active_db_sessions:
            if db_session.session_id not in active_session_ids:
                duration = self._session_durations.get(
                    db_session.session_id, db_session.play_duration_seconds
                )
                await db.end_session(db_session.session_id, duration)
                self._session_durations.pop(db_session.session_id, None)
                logger.info(
                    f"Session ended: {db_session.user_name} - {db_session.media_title}"
                )

        if self._on_session_update:
            await self._on_session_update()

    async def _handle_playback_start(self, data: dict) -> None:
        """Handle PlaybackStart event."""
        session_id = data.get("SessionId", "")
        if not session_id:
            return

        now_playing = data.get("Item", {})
        if not now_playing:
            return

        # Build a session-like structure for extraction
        session_data = {
            "Id": session_id,
            "UserId": data.get("UserId", ""),
            "UserName": data.get("Username", ""),
            "DeviceId": data.get("DeviceId", ""),
            "DeviceName": data.get("DeviceName", ""),
            "Client": data.get("Client", ""),
        }

        event = self._extract_playback_event(session_data, now_playing)
        existing = await db.get_active_session(session_id)
        if not existing:
            await self._create_session(event)

    async def _handle_playback_stop(self, data: dict) -> None:
        """Handle PlaybackStopped event."""
        session_id = data.get("SessionId", "")
        if not session_id:
            return

        play_state = data.get("PlayState", {})
        position_ticks = play_state.get("PositionTicks", 0)
        duration_seconds = position_ticks // 10_000_000

        # Use tracked duration if available
        final_duration = self._session_durations.get(session_id, duration_seconds)
        await db.end_session(session_id, final_duration)
        self._session_durations.pop(session_id, None)

        logger.info(f"Playback stopped for session {session_id}")

        if self._on_session_update:
            await self._on_session_update()

    def _extract_playback_event(
        self, session_data: dict, now_playing: dict
    ) -> PlaybackEvent:
        """Extract a PlaybackEvent from session and item data."""
        return PlaybackEvent(
            session_id=session_data.get("Id", ""),
            user_id=session_data.get("UserId", ""),
            user_name=session_data.get("UserName", "Unknown"),
            device_id=session_data.get("DeviceId", ""),
            device_name=session_data.get("DeviceName", "Unknown"),
            client_name=session_data.get("Client", "Unknown"),
            item_id=now_playing.get("Id", ""),
            item_name=now_playing.get("Name", "Unknown"),
            item_type=now_playing.get("Type", "Unknown"),
            series_name=now_playing.get("SeriesName"),
            season_number=now_playing.get("ParentIndexNumber"),
            episode_number=now_playing.get("IndexNumber"),
        )

    async def _create_session(self, event: PlaybackEvent) -> None:
        """Create a new session from a playback event."""
        now = datetime.now()
        session = Session(
            session_id=event.session_id,
            user_id=event.user_id,
            user_name=event.user_name,
            device_id=event.device_id,
            device_name=event.device_name,
            client_name=event.client_name,
            media_id=event.item_id,
            media_title=event.item_name,
            media_type=event.item_type,
            series_name=event.series_name,
            season_number=event.season_number,
            episode_number=event.episode_number,
            started_at=now,
            last_progress_update=now,
        )
        await db.create_session(session)
        self._session_durations[event.session_id] = 0
        logger.info(
            f"Session started: {event.user_name} - {event.item_name} "
            f"on {event.device_name}"
        )


# Global client instance
jellyfin_client = JellyfinWebSocketClient()
