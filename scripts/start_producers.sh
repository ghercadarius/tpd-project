#!/usr/bin/env bash
# Launch producers in replay/live/both modes.
#   --mode replay   --file <path>    [--rate max|realtime|<float>]
#   --mode live
#   --mode both     --file <path>
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
HERE="$(repo_root)"
cd "$HERE"

PYTHON="$(resolve_python "$HERE")"

MODE="replay"
FILE=""
RATE="max"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2 ;;
    --file) FILE="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

run_replay() {
  if [ -z "$FILE" ]; then echo "--file is required for replay" >&2; exit 1; fi
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
