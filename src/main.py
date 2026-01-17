import argparse
import asyncio
import logging
import signal
import sys

import uvicorn

from .config import settings
from .database import db
from .jellyfin_client import jellyfin_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class JellytrackServer:
    def __init__(self):
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """Start the Jellytrack server."""
        logger.info("Starting Jellytrack...")

        # Connect to database
        await db.connect()
        logger.info(f"Connected to database: {settings.database_path}")

        # Setup signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        # Start WebSocket client and web server
        ws_task = asyncio.create_task(self._run_websocket_client())
        web_task = asyncio.create_task(self._run_web_server())

        logger.info(f"Dashboard available at http://localhost:{settings.dashboard_port}")

        # Wait for shutdown
        await self._shutdown_event.wait()

        # Cancel tasks
        ws_task.cancel()
        web_task.cancel()

        try:
            await ws_task
        except asyncio.CancelledError:
            pass

        try:
            await web_task
        except asyncio.CancelledError:
            pass

        # Cleanup
        await jellyfin_client.stop()
        await db.close()
        logger.info("Jellytrack stopped")

    async def shutdown(self) -> None:
        """Signal shutdown."""
        logger.info("Shutting down...")
        self._shutdown_event.set()

    async def _run_websocket_client(self) -> None:
        """Run the Jellyfin WebSocket client."""
        try:
            await jellyfin_client.start()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"WebSocket client error: {e}")

    async def _run_web_server(self) -> None:
        """Run the FastAPI web server."""
        from dashboard.app import app

        config = uvicorn.Config(
            app,
            host="0.0.0.0",
            port=settings.dashboard_port,
            log_level="info",
        )
        server = uvicorn.Server(config)
        try:
            await server.serve()
        except asyncio.CancelledError:
            pass


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Jellytrack - Jellyfin Playback Tracker")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Import command
    import_parser = subparsers.add_parser(
        "import", help="Import historical data from Playback Reporting plugin"
    )
    import_parser.add_argument(
        "--days",
        type=int,
        default=365,
        help="Number of days to import (default: 365)",
    )

    args = parser.parse_args()

    if not settings.jellyfin_api_key:
        logger.error("JELLYFIN_API_KEY is not set. Please set it in .env file.")
        sys.exit(1)

    if args.command == "import":
        from .importer import run_import

        count = asyncio.run(run_import(days=args.days))
        logger.info(f"Imported {count} sessions")
    else:
        server = JellytrackServer()
        asyncio.run(server.start())


if __name__ == "__main__":
    main()
