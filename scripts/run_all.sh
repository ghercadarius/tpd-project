#!/usr/bin/env bash
# End-to-end orchestrator for a demo run.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
HERE="$(repo_root)"
cd "$HERE"

bash scripts/setup.sh
bash scripts/start_infra.sh

PYTHON="$(resolve_python "$HERE")"

if [ ! -f model/artifacts/sentiment.int8.onnx ]; then
  echo "[run_all] no model artifact found; exporting pretrained checkpoint"
  "$PYTHON" scripts/export_model.py
fi

if [ "${MODE:-replay}" = "live" ]; then
  bash scripts/start_producers.sh --mode live &
else
  if [ -z "${FILE:-}" ]; then
    echo "set FILE=path/to/dump.ndjson for replay mode" >&2
    exit 1
  fi
  bash scripts/start_producers.sh --mode replay --file "$FILE" --rate "${RATE:-max}" &
fi

bash scripts/submit_flink_job.sh
bash scripts/start_dashboard.sh
