from fastapi import FastAPI

from .routes import router

app = FastAPI(title="Jellytrack", description="Jellyfin Playback Tracker")

# Include routes (template filters are registered in routes.py)
app.include_router(router)
