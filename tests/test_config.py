from src.config import Settings


def test_jellyfin_ws_url():
    settings = Settings(
        jellyfin_url="http://example.test:8096",
        jellyfin_api_key="abc123",
    )
    assert settings.jellyfin_ws_url == "ws://example.test:8096/socket?api_key=abc123"
