from pathlib import Path

from fastapi import FastAPI
from fastapi.templating import Jinja2Templates

from .routes import router

app = FastAPI(title="Jellytrack", description="Jellyfin Playback Tracker")

# Templates
templates_dir = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=templates_dir)

# Include routes
app.include_router(router)


def format_duration(seconds: int) -> str:
    """Format seconds as human readable duration."""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        minutes = seconds // 60
        return f"{minutes}m"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        if minutes > 0:
            return f"{hours}h {minutes}m"
        return f"{hours}h"


# Add custom filter to templates
templates.env.filters["duration"] = format_duration
