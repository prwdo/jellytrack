import json
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest

from src.config import settings
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
    heatmap = await db.get_hourly_weekday_heatmap(
        days=query_days,
        user_id=user_id,
        device_name=device_name,
        media_type=media_type,
    )
    series_days = min(query_days, 90)
    series_daily = await db.get_series_daily_totals(
        days=series_days,
        user_id=user_id,
        device_name=device_name,
        media_type=media_type,
    )
    metrics_days = query_days if settings.retention_days <= 0 else min(
        query_days, settings.retention_days
    )
    sessions_for_metrics = await db.get_sessions_for_metrics(
        days=metrics_days,
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

    # Heatmap data (weekday x hour)
    heatmap_points = []
    heatmap_max = 0
    heatmap_lookup = {(h["weekday"], h["hour"]): h["session_count"] for h in heatmap}
    for weekday in range(7):
        for hour in range(24):
            count = heatmap_lookup.get((weekday, hour), 0)
            heatmap_max = max(heatmap_max, count)
            heatmap_points.append({"x": hour, "y": weekday, "v": count})

    # Session length distribution
    length_bins = [
        ("<5m", 0, 5 * 60),
        ("5-15m", 5 * 60, 15 * 60),
        ("15-30m", 15 * 60, 30 * 60),
        ("30-60m", 30 * 60, 60 * 60),
        ("1-2h", 60 * 60, 120 * 60),
        ("2h+", 120 * 60, None),
    ]
    length_labels = [label for label, _, _ in length_bins]
    length_counts = [0 for _ in length_bins]
    for session in sessions_for_metrics:
        if session["is_active"]:
            continue
        total_seconds = session["play_seconds"] + session["paused_seconds"]
        if total_seconds <= 0:
            continue
        for idx, (_, min_seconds, max_seconds) in enumerate(length_bins):
            if max_seconds is None and total_seconds >= min_seconds:
                length_counts[idx] += 1
                break
            if max_seconds is not None and min_seconds <= total_seconds < max_seconds:
                length_counts[idx] += 1
                break

    # Concurrent sessions (daily peak)
    concurrent_days = min(metrics_days, 90)
    now = datetime.now()
    since = now - timedelta(days=concurrent_days)
    since_hour = since.replace(minute=0, second=0, microsecond=0)
    total_hours = int((now - since_hour).total_seconds() // 3600) + 1
    concurrent_hours = [0 for _ in range(total_hours)]
    for session in sessions_for_metrics:
        started_at = datetime.fromisoformat(session["started_at"])
        if session["ended_at"]:
            ended_at = datetime.fromisoformat(session["ended_at"])
        elif session["last_progress_update"]:
            ended_at = datetime.fromisoformat(session["last_progress_update"])
        else:
            ended_at = now
        if session["is_active"]:
            ended_at = now
        if ended_at < since_hour or started_at > now:
            continue
        start = max(started_at, since_hour)
        end = min(ended_at, now)
        start_idx = int((start - since_hour).total_seconds() // 3600)
        end_idx = int((end - since_hour).total_seconds() // 3600)
        for idx in range(start_idx, end_idx + 1):
            concurrent_hours[idx] += 1
    concurrent_labels = []
    concurrent_peaks = []
    current_day = None
    current_peak = 0
    for idx, count in enumerate(concurrent_hours):
        bucket_time = since_hour + timedelta(hours=idx)
        day_label = bucket_time.date().isoformat()
        if current_day is None:
            current_day = day_label
        if day_label != current_day:
            concurrent_labels.append(current_day)
            concurrent_peaks.append(current_peak)
            current_day = day_label
            current_peak = count
        else:
            current_peak = max(current_peak, count)
    if current_day is not None:
        concurrent_labels.append(current_day)
        concurrent_peaks.append(current_peak)

    # Series trends (top 5 series)
    series_totals: dict[str, int] = {}
    for row in series_daily:
        series_totals[row["series_name"]] = (
            series_totals.get(row["series_name"], 0) + row["total_seconds"]
        )
    top_series = [
        name for name, _ in sorted(series_totals.items(), key=lambda i: i[1], reverse=True)[:5]
    ]
    series_labels = daily_labels
    series_index = {label: idx for idx, label in enumerate(series_labels)}
    series_data_map = {name: [0 for _ in series_labels] for name in top_series}
    for row in series_daily:
        name = row["series_name"]
        if name not in series_data_map:
            continue
        idx = series_index.get(row["date"])
        if idx is None:
            continue
        series_data_map[name][idx] = round(row["total_seconds"] / 3600, 2)
    series_colors = [
        "rgba(59, 130, 246, 0.35)",
        "rgba(34, 197, 94, 0.35)",
        "rgba(234, 179, 8, 0.35)",
        "rgba(239, 68, 68, 0.35)",
        "rgba(147, 51, 234, 0.35)",
    ]
    series_border_colors = [
        "rgba(59, 130, 246, 0.9)",
        "rgba(34, 197, 94, 0.9)",
        "rgba(234, 179, 8, 0.9)",
        "rgba(239, 68, 68, 0.9)",
        "rgba(147, 51, 234, 0.9)",
    ]
    series_datasets = []
    for idx, name in enumerate(top_series):
        series_datasets.append(
            {
                "label": name,
                "data": series_data_map[name],
                "borderColor": series_border_colors[idx % len(series_border_colors)],
                "backgroundColor": series_colors[idx % len(series_colors)],
                "fill": True,
                "tension": 0.3,
                "pointRadius": 2,
            }
        )

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
        request,
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
            "heatmap_json": json.dumps(heatmap_points),
            "heatmap_max": heatmap_max,
            "length_labels_json": json.dumps(length_labels),
            "length_counts_json": json.dumps(length_counts),
            "concurrent_labels_json": json.dumps(concurrent_labels),
            "concurrent_counts_json": json.dumps(concurrent_peaks),
            "series_labels_json": json.dumps(series_labels),
            "series_datasets_json": json.dumps(series_datasets),
        },
    )


@router.get("/user/{user_id}", response_class=HTMLResponse)
async def user_detail(request: Request, user_id: str):
    """User detail page."""
    user_stats = await db.get_user_stats(user_id)
    return templates.TemplateResponse(
        request,
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
        request,
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
        request,
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
        request,
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
        request,
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
