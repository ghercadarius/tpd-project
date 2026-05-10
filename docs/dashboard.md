# Dashboard

Two processes back the dashboard:

1. **`dashboard/api.py`** — FastAPI service on `:8000`.
2. **`dashboard/app.py`** — Streamlit UI on `:8501`.
3. **`dashboard/sink_consumer.py`** — sidecar that materializes Kafka
   `reddit.aggregates` and `reddit.alerts` into Postgres for the API to query.

Run all three with `bash scripts/start_dashboard.sh`.

## API endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/brands` | Distinct brands present in Postgres. |
| GET | `/aggregates?brand=&minutes=` | Time-series of 5-min aggregates. |
| GET | `/alerts?active=true&limit=` | Recent alerts (active=last hour). |
| GET | `/stream/alerts` | Server-Sent Events tailing `reddit.alerts`. |
| GET | `/health` | `{ "ok": true }`. |

## Streamlit UI
- **Sidebar**: brand selector, lookback window, auto-refresh toggle.
- **Active alerts panel**: last-hour alerts with severity emoji and a sample
  comment.
- **Trend charts**: `volume`/`neg_count` and `neg_ratio`/`avg_neg_prob` over
  the chosen lookback.
- **Recent alerts table**: full table for cross-brand inspection.

## Postgres schema
See [postgres-init.sql](../infra/postgres-init.sql). Two tables:
- `aggregates_5m (brand, window_start PK, …)`
- `alerts (id, brand, triggered_at, …, severity)`
