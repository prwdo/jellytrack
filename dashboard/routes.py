import json
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest

from src.database import db
from src.jellyfin_client import jellyfin_client

router = APIRouter()

templates_dir = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=templates_dir)

ACTIVE_SESSIONS = Gauge("jellytrack_active_sessions", "Active playback sessions")
TOTAL_SESSIONS = Gauge("jellytrack_total_sessions", "Total sessions tracked")
WS_CONNECTED = Gauge("jellytrack_ws_connected", "Jellyfin websocket connected")
LAST_WS_MESSAGE = Gauge(
    "jellytrack_ws_last_message_timestamp", "Last websocket message unix timestamp"
)


def format_duration(seconds: int) -> str:
    """Format seconds as human readable duration."""
    if not seconds:
        return "0s"
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


def format_duration_long(seconds: int) -> str:
    """Format seconds as detailed duration."""
    if not seconds:
        return "0 seconds"

    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60

    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")

    return " ".join(parts) if parts else "< 1m"


def timeago(dt) -> str:
    """Format datetime as relative time."""
    from datetime import datetime

    if not dt:
        return "Unknown"

    # Make dt timezone-naive if it has timezone info
    if hasattr(dt, "tzinfo") and dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)

    now = datetime.now()
    diff = now - dt

    seconds = diff.total_seconds()
    if seconds < 60:
        return "just now"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        return f"{minutes}m ago"
    elif seconds < 86400:
        hours = int(seconds // 3600)
        return f"{hours}h ago"
    else:
        days = int(seconds // 86400)
        return f"{days}d ago"


templates.env.filters["duration"] = format_duration
templates.env.filters["duration_long"] = format_duration_long
templates.env.filters["timeago"] = timeago


def _normalize_filter(value: str | None) -> str | None:
    if not value or value == "all":
        return None
    return value


def _percent_delta(current: int, previous: int) -> float | None:
    if previous <= 0:
        return None
    return round(((current - previous) / previous) * 100, 1)


@router.get("/", response_class=HTMLResponse)
async def index(request: Request, days: int = 30):
    """Main dashboard page."""
    # Validate days parameter
    valid_periods = [7, 30, 90, 365, 0]  # 0 = all time
    if days not in valid_periods:
        days = 30

    # For "all time", use a large number
    query_days = days if days > 0 else 3650

    user_id = _normalize_filter(request.query_params.get("user_id"))
    device_name = _normalize_filter(request.query_params.get("device_name"))
    media_type = _normalize_filter(request.query_params.get("media_type"))

    sessions = await db.get_active_sessions(
        user_id=user_id, device_name=device_name, media_type=media_type
    )
    watchtime = await db.get_user_watchtime(
        days=query_days,
        user_id=user_id,
        device_name=device_name,
        media_type=media_type,
    )
    top_media = await db.get_top_media(
        days=query_days,
        user_id=user_id,
        device_name=device_name,
        media_type=media_type,
    )
    hourly = await db.get_hourly_stats(
        days=query_days,
        user_id=user_id,
        device_name=device_name,
        media_type=media_type,
    )
    devices = await db.get_device_stats(
        days=query_days,
        user_id=user_id,
        device_name=device_name,
        media_type=media_type,
    )
    summary = await db.get_summary_stats(
        days=query_days,
        user_id=user_id,
        device_name=device_name,
        media_type=media_type,
    )
    media_types = await db.get_media_type_stats(
        days=query_days,
        user_id=user_id,
        device_name=device_name,
        media_type=media_type,
    )
    recent = await db.get_recent_activity(
        limit=15,
        user_id=user_id,
        device_name=device_name,
        media_type=media_type,
    )
    daily = await db.get_daily_stats(
        days=min(query_days, 90),
        user_id=user_id,
        device_name=device_name,
        media_type=media_type,
    )
    pause_stats = await db.get_pause_stats(
        days=query_days,
        user_id=user_id,
        device_name=device_name,
        media_type=media_type,
    )
    filters = await db.get_filter_options(days=query_days)

    # Prepare chart data as JSON
    # Hourly chart data - ensure all 24 hours are represented
    hourly_data = [0] * 24
    for h in hourly:
        hourly_data[h.hour] = h.session_count

    # Daily chart data
    daily_labels = [d["date"] for d in daily]
    daily_sessions = [d["session_count"] for d in daily]
    daily_hours = [round(d["total_seconds"] / 3600, 1) for d in daily]

    # Media types for pie chart
    media_type_labels = [mt["media_type"] for mt in media_types]
    media_type_values = [mt["total_seconds"] for mt in media_types]

    # User watchtime for bar chart
    user_labels = [u.user_name for u in watchtime[:10]]
    user_hours = [round(u.total_seconds / 3600, 1) for u in watchtime[:10]]

    # Device data for pie chart
    device_labels = [d.device_name for d in devices[:8]]
    device_values = [d.total_seconds for d in devices[:8]]

    # Highlights
    highlight_user = watchtime[0] if watchtime else None
    highlight_media = top_media[0] if top_media else None
    highlight_device = devices[0] if devices else None

    # Period label for display
    period_labels = {7: "7 days", 30: "30 days", 90: "90 days", 365: "1 year", 0: "All time"}
    period_label = period_labels.get(days, "30 days")

    filter_params = {
        key: value
        for key, value in {
            "user_id": user_id,
            "device_name": device_name,
            "media_type": media_type,
        }.items()
        if value
    }
    filter_query = urlencode(filter_params)

    trend = None
    if days > 0:
        current = summary
        previous_total = await db.get_summary_stats(
            days=query_days * 2,
            user_id=user_id,
            device_name=device_name,
            media_type=media_type,
        )
        prev_sessions = max(0, previous_total["total_sessions"] - current["total_sessions"])
        prev_seconds = max(0, previous_total["total_seconds"] - current["total_seconds"])
        trend = {
            "sessions": _percent_delta(current["total_sessions"], prev_sessions),
            "watchtime": _percent_delta(current["total_seconds"], prev_seconds),
        }

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "sessions": sessions,
            "watchtime": watchtime,
            "top_media": top_media,
            "devices": devices,
            "summary": summary,
            "media_types": media_types,
            "recent": recent,
            "pause_stats": pause_stats,
            "filters": filters,
            "selected_filters": {
                "user_id": user_id,
                "device_name": device_name,
                "media_type": media_type,
            },
            "filter_query": filter_query,
            "highlights": {
                "user": highlight_user,
                "media": highlight_media,
                "device": highlight_device,
            },
            "trend": trend,
            # Time period
            "selected_days": days,
            "period_label": period_label,
            # Chart data as JSON strings
            "hourly_data_json": json.dumps(hourly_data),
            "daily_labels_json": json.dumps(daily_labels),
            "daily_sessions_json": json.dumps(daily_sessions),
            "daily_hours_json": json.dumps(daily_hours),
            "media_type_labels_json": json.dumps(media_type_labels),
            "media_type_values_json": json.dumps(media_type_values),
            "user_labels_json": json.dumps(user_labels),
            "user_hours_json": json.dumps(user_hours),
            "device_labels_json": json.dumps(device_labels),
            "device_values_json": json.dumps(device_values),
        },
    )


@router.get("/user/{user_id}", response_class=HTMLResponse)
async def user_detail(request: Request, user_id: str):
    """User detail page."""
    user_stats = await db.get_user_stats(user_id)
    return templates.TemplateResponse(
        "user.html",
        {
            "request": request,
            "user": user_stats,
        },
    )


@router.get("/api/sessions/active", response_class=HTMLResponse)
async def active_sessions(request: Request):
    """Get active sessions partial for HTMX."""
    user_id = _normalize_filter(request.query_params.get("user_id"))
    device_name = _normalize_filter(request.query_params.get("device_name"))
    media_type = _normalize_filter(request.query_params.get("media_type"))
    sessions = await db.get_active_sessions(
        user_id=user_id, device_name=device_name, media_type=media_type
    )
    return templates.TemplateResponse(
        "partials/active_sessions.html",
        {"request": request, "sessions": sessions},
    )


@router.get("/api/stats/watchtime", response_class=HTMLResponse)
async def watchtime_stats(request: Request, days: int = 30):
    """Get watchtime stats partial for HTMX."""
    user_id = _normalize_filter(request.query_params.get("user_id"))
    device_name = _normalize_filter(request.query_params.get("device_name"))
    media_type = _normalize_filter(request.query_params.get("media_type"))
    watchtime = await db.get_user_watchtime(
        days=days, user_id=user_id, device_name=device_name, media_type=media_type
    )
    return templates.TemplateResponse(
        "partials/stats.html",
        {"request": request, "watchtime": watchtime},
    )


@router.get("/api/stats/top-media", response_class=HTMLResponse)
async def top_media_stats(request: Request, days: int = 30):
    """Get top media partial for HTMX."""
    user_id = _normalize_filter(request.query_params.get("user_id"))
    device_name = _normalize_filter(request.query_params.get("device_name"))
    media_type = _normalize_filter(request.query_params.get("media_type"))
    top_media = await db.get_top_media(
        days=days, user_id=user_id, device_name=device_name, media_type=media_type
    )
    return templates.TemplateResponse(
        "partials/top_media.html",
        {"request": request, "top_media": top_media},
    )


@router.get("/api/stats/recent", response_class=HTMLResponse)
async def recent_activity(request: Request):
    """Get recent activity partial for HTMX."""
    user_id = _normalize_filter(request.query_params.get("user_id"))
    device_name = _normalize_filter(request.query_params.get("device_name"))
    media_type = _normalize_filter(request.query_params.get("media_type"))
    recent = await db.get_recent_activity(
        limit=15, user_id=user_id, device_name=device_name, media_type=media_type
    )
    return templates.TemplateResponse(
        "partials/recent_activity.html",
        {"request": request, "recent": recent},
    )


@router.get("/api/stats/hourly")
async def hourly_stats(request: Request, days: int = 30):
    """Get hourly usage stats as JSON for charts."""
    user_id = _normalize_filter(request.query_params.get("user_id"))
    device_name = _normalize_filter(request.query_params.get("device_name"))
    media_type = _normalize_filter(request.query_params.get("media_type"))
    hourly = await db.get_hourly_stats(
        days=days, user_id=user_id, device_name=device_name, media_type=media_type
    )
    return [h.model_dump() for h in hourly]


@router.get("/api/stats/devices")
async def device_stats(request: Request, days: int = 30):
    """Get device stats as JSON."""
    user_id = _normalize_filter(request.query_params.get("user_id"))
    device_name = _normalize_filter(request.query_params.get("device_name"))
    media_type = _normalize_filter(request.query_params.get("media_type"))
    devices = await db.get_device_stats(
        days=days, user_id=user_id, device_name=device_name, media_type=media_type
    )
    return [d.model_dump() for d in devices]


@router.get("/health")
async def health():
    """Basic health check."""
    status = jellyfin_client.status()
    try:
        _ = db.conn
        db_connected = True
    except RuntimeError:
        db_connected = False
    return {
        "status": "ok",
        "db_connected": db_connected,
        "ws_connected": status["connected"],
        "ws_last_message_at": status["last_message_at"],
    }


@router.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    active = await db.get_active_sessions()
    summary = await db.get_summary_stats(days=36500)
    status = jellyfin_client.status()

    ACTIVE_SESSIONS.set(len(active))
    TOTAL_SESSIONS.set(summary["total_sessions"])
    WS_CONNECTED.set(1 if status["connected"] else 0)
    if status["last_message_at"]:
        LAST_WS_MESSAGE.set(datetime.fromisoformat(status["last_message_at"]).timestamp())
    else:
        LAST_WS_MESSAGE.set(0)

    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)
