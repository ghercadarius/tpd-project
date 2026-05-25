#!/usr/bin/env bash
# Launch producers in replay/live/both modes.
#   --mode replay   --file <path>    [--rate max|realtime|<float>]
#   --mode replay   --dataset-dir <dir>  (expects submissions/ and comments/)
#   --mode live
#   --mode both     --file <path>
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
HERE="$(repo_root)"
cd "$HERE"

if [ -f ".env" ]; then
  set -a
  source ".env"
  set +a
fi

PYTHON="$(resolve_python "$HERE")"

MODE="replay"
FILE=""
DATASET_DIR=""
RATE="max"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2 ;;
    --file) FILE="$2"; shift 2 ;;
    --dataset-dir) DATASET_DIR="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [ -z "$DATASET_DIR" ] && [ -n "${REPLAY_DATASET_DIR:-}" ]; then
  DATASET_DIR="$REPLAY_DATASET_DIR"
fi

run_replay() {
  if [ -n "$DATASET_DIR" ]; then
    local submissions comments
    shopt -s nullglob
    submissions=("$DATASET_DIR"/submissions/*.zst "$DATASET_DIR"/submissions/*.ndjson "$DATASET_DIR"/submissions/*.jsonl "$DATASET_DIR"/submissions/*.csv)
    comments=("$DATASET_DIR"/comments/*.zst "$DATASET_DIR"/comments/*.ndjson "$DATASET_DIR"/comments/*.jsonl "$DATASET_DIR"/comments/*.csv)
    shopt -u nullglob

    if [ ${#submissions[@]} -eq 0 ] || [ ${#comments[@]} -eq 0 ]; then
      echo "--dataset-dir must contain both submissions/ and comments/ files" >&2
      exit 1
    fi

    for file in "${submissions[@]}"; do
      "$PYTHON" -m producers.pushshift_replay --file "$file" --rate "$RATE" &
    done
    for file in "${comments[@]}"; do
      "$PYTHON" -m producers.pushshift_replay --file "$file" --rate "$RATE" &
    done
    wait
    return 0
  fi

  if [ -z "$FILE" ]; then echo "--file or --dataset-dir is required for replay" >&2; exit 1; fi
  "$PYTHON" -m producers.pushshift_replay --file "$FILE" --rate "$RATE"
}
run_live() {
  "$PYTHON" -m producers.reddit_live --kind submissions &
  "$PYTHON" -m producers.reddit_live --kind comments &
  wait
}

case "$MODE" in
  replay) run_replay ;;
  live)   run_live ;;
  both)   run_replay & run_live & wait ;;
  *) echo "invalid mode: $MODE" >&2; exit 1 ;;
esac
