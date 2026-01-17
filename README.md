# jellytrack

Jellytrack tracks Jellyfin playback sessions in SQLite and serves a small dashboard for activity and stats.

## Features
- Live session tracking via Jellyfin WebSocket
- Historical import from Playback Reporting plugin
- Dashboard with user, device, and media stats
- Retention with daily/hourly aggregation for older sessions
- Health and Prometheus metrics endpoints

## Requirements
- Python 3.12+
- Jellyfin server with an API key
- (Optional) Playback Reporting plugin for historical import

## Setup
1. Create a `.env` file with:
   - `JELLYFIN_URL` (e.g. `http://localhost:8096`)
   - `JELLYFIN_API_KEY`
   - `RETENTION_DAYS` (optional, default: 180)
   - `AGGREGATION_INTERVAL_HOURS` (optional, default: 24)
     - set `RETENTION_DAYS=0` to disable pruning
2. Install deps:
```bash
pip install -r requirements.txt
```

## Run
```bash
python -m src.main
```

Dashboard: `http://localhost:8085`

## Import historical data
```bash
python -m src.main import --days 365
```

## Development
Install dev tools:
```bash
pip install -r requirements-dev.txt
```

Lint:
```bash
ruff check .
```

Format:
```bash
ruff format
```

## Observability
- Health check: `GET /health`
- Prometheus metrics: `GET /metrics`

## Docker
```bash
docker compose up --build
```
