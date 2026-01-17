import hashlib
import logging
from datetime import datetime

import httpx

from .config import settings
from .database import db
from .models import Session

logger = logging.getLogger(__name__)


class PlaybackReportingImporter:
    """Import historical data from Jellyfin Playback Reporting plugin."""

    def __init__(self):
        self.base_url = settings.jellyfin_url
        self.api_key = settings.jellyfin_api_key

    async def import_all(self, days: int = 365) -> int:
        """Import all playback activity from the last N days."""
        logger.info(f"Importing playback data from last {days} days...")

        # Query the Playback Reporting plugin
        query = f"""
            SELECT
                rowid,
                DateCreated,
                UserId,
                ItemId,
                ItemType,
                ItemName,
                PlaybackMethod,
                ClientName,
                DeviceName,
                PlayDuration
            FROM PlaybackActivity
            WHERE DateCreated >= datetime('now', '-{days} days')
            ORDER BY DateCreated ASC
        """

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/user_usage_stats/submit_custom_query",
                params={"api_key": self.api_key},
                json={"CustomQueryString": query},
                timeout=60.0,
            )

            if response.status_code != 200:
                logger.error(f"Failed to query Playback Reporting: {response.status_code}")
                return 0

            data = response.json()

        columns = data.get("columns") or data.get("colums") or []
        results = data.get("results", [])

        if not columns:
            logger.error("Playback Reporting response missing columns")
            return 0

        if not results:
            logger.info("No playback data found to import")
            return 0

        # Get user names mapping
        user_names = await self._get_user_names()

        imported = 0
        skipped = 0

        for row in results:
            row_dict = dict(zip(columns, row))

            # Generate a stable session ID (prefer rowid when available)
            rowid = row_dict.get("rowid")
            if rowid:
                session_id = f"imported_{rowid}"
            else:
                fingerprint = "|".join(str(row_dict.get(k, "")) for k in columns)
                digest = hashlib.sha1(fingerprint.encode("utf-8")).hexdigest()
                session_id = f"imported_{digest}"

            # Check if already imported
            existing = await db.get_session_by_id(session_id)
            if existing:
                skipped += 1
                continue

            # Parse the date
            date_str = row_dict.get("DateCreated", "")
            try:
                started_at = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            except ValueError:
                try:
                    started_at = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")
                except ValueError:
                    started_at = datetime.strptime(date_str[:19], "%Y-%m-%d %H:%M:%S")

            # Parse item name to extract series info
            item_name = row_dict.get("ItemName", "Unknown")
            series_name = None
            season_number = None
            episode_number = None
            media_title = item_name

            # Try to parse episode format: "Series - s01e02 - Episode Title"
            if " - s" in item_name and "e" in item_name:
                parts = item_name.split(" - ", 2)
                if len(parts) >= 2:
                    series_name = parts[0]
                    season_ep = parts[1]
                    if len(parts) >= 3:
                        media_title = parts[2]
                    else:
                        media_title = item_name

                    # Parse s01e02 format
                    try:
                        se_part = season_ep.lower()
                        if se_part.startswith("s") and "e" in se_part:
                            s_idx = se_part.index("s")
                            e_idx = se_part.index("e")
                            season_number = int(se_part[s_idx + 1 : e_idx])
                            episode_number = int(se_part[e_idx + 1 :])
                    except (ValueError, IndexError):
                        pass

            user_id = row_dict.get("UserId", "")
            play_duration = int(row_dict.get("PlayDuration", 0))

            session = Session(
                session_id=session_id,
                user_id=user_id,
                user_name=user_names.get(user_id, "Unknown"),
                device_id=f"imported_{row_dict.get('DeviceName', 'unknown')}",
                device_name=row_dict.get("DeviceName", "Unknown"),
                client_name=row_dict.get("ClientName", "Unknown"),
                media_id=row_dict.get("ItemId", ""),
                media_title=media_title,
                media_type=row_dict.get("ItemType", "Unknown"),
                series_name=series_name,
                season_number=season_number,
                episode_number=episode_number,
                started_at=started_at,
                ended_at=started_at,  # Historical data doesn't have end time
                play_duration_seconds=play_duration,
                is_active=False,
                last_progress_update=started_at,
            )

            try:
                await db.create_session(session)
                imported += 1
            except Exception as e:
                logger.warning(f"Failed to import session: {e}")
                skipped += 1

        logger.info(f"Import complete: {imported} imported, {skipped} skipped")
        return imported

    async def _get_user_names(self) -> dict[str, str]:
        """Get mapping of user IDs to names."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/Users",
                params={"api_key": self.api_key},
                timeout=30.0,
            )

            if response.status_code != 200:
                return {}

            users = response.json()
            return {u["Id"]: u["Name"] for u in users}


async def run_import(days: int = 365) -> int:
    """Run the import process."""
    await db.connect()
    try:
        importer = PlaybackReportingImporter()
        return await importer.import_all(days=days)
    finally:
        await db.close()
