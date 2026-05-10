#!/usr/bin/env bash
# Run the FastAPI backend, the alerts->Postgres sink, and Streamlit in parallel.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$HERE"

trap 'kill 0' EXIT

python -m dashboard.sink_consumer &
uvicorn dashboard.api:app --host 0.0.0.0 --port 8000 &
streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0 &

wait
