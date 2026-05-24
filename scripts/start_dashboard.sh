#!/usr/bin/env bash
# Run the FastAPI backend, the alerts->Postgres sink, and Streamlit in parallel.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
HERE="$(repo_root)"
cd "$HERE"

PYTHON="$(resolve_python "$HERE")"

trap 'kill 0' EXIT

"$PYTHON" -m dashboard.sink_consumer &
"$PYTHON" -m uvicorn dashboard.api:app --host 0.0.0.0 --port 8000 &
"$PYTHON" -m streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0 &

wait
