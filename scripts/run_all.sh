#!/usr/bin/env bash
# End-to-end orchestrator.
#
# Usage:
#   FILE=/path/to/dump.zst bash scripts/run_all.sh
#   FILE=/path/to/dump.zst RATE=10 bash scripts/run_all.sh   # 10× speed
#
# Prerequisites: Docker running, Python 3.10/3.11 venv activated, Java 11+.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

# Resolve python
PYTHON="${PYTHON:-python3}"
if [ -f .venv/bin/python ]; then PYTHON=".venv/bin/python"; fi

echo "==> Starting infrastructure (Docker) …"
docker compose -f infra/docker-compose.yml up -d --wait

echo "==> Waiting for Kafka …"
until docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  sleep 2
done

echo "==> Creating Kafka topics …"
"$PYTHON" scripts/init_kafka.py

echo "==> Starting Postgres sink consumer …"
"$PYTHON" -m dashboard.sink_consumer &
SINK_PID=$!

echo "==> Starting Flink job (local mini-cluster) …"
"$PYTHON" -m flink_jobs.brand_crisis_job &
FLINK_PID=$!

echo "==> Starting FastAPI backend …"
"$PYTHON" -m uvicorn dashboard.api:app --host 0.0.0.0 --port 8000 &
API_PID=$!

echo "==> Starting Streamlit dashboard …"
"$PYTHON" -m streamlit run dashboard/app.py --server.port 8501 &
UI_PID=$!

echo ""
echo "Dashboard: http://localhost:8501"
echo "API:       http://localhost:8000"
echo "Kafka UI:  http://localhost:18080"
echo ""

if [ -z "${FILE:-}" ]; then
  echo "No FILE set — skipping producer.  Set FILE=/path/to/dump.zst and re-run to ingest data."
else
  echo "==> Replaying $FILE …"
  "$PYTHON" -m producers.pushshift_replay --file "$FILE" --rate "${RATE:-max}"
fi

wait $FLINK_PID $SINK_PID $API_PID $UI_PID
