from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    jellyfin_url: str = "http://localhost:8096"
    jellyfin_api_key: str = ""
    database_path: str = "./data/jellytrack.db"
    dashboard_port: int = 8085
    session_timeout_minutes: int = 5
    retention_days: int = 180
    aggregation_interval_hours: int = 24

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @property
    def jellyfin_ws_url(self) -> str:
        """Get WebSocket URL from HTTP URL."""
        url = self.jellyfin_url.replace("http://", "ws://").replace("https://", "wss://")
        return f"{url}/socket?api_key={self.jellyfin_api_key}"

    @property
    def database_path_resolved(self) -> Path:
        """Get resolved database path."""
        path = Path(self.database_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path


settings = Settings()
