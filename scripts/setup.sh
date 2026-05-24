#!/usr/bin/env bash
# Bootstrap a Python venv, install deps, and prepare the .env file.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
HERE="$(repo_root)"
cd "$HERE"

PYTHON="$(resolve_python "$HERE")"

echo "[setup] Python venv"
if [ ! -d ".venv" ]; then
  "$PYTHON" -m venv .venv
else
  echo "env already exists"
fi
PYTHON="$HERE/.venv/bin/python"

echo "[setup] pip install"
"$PYTHON" -m pip install --upgrade pip
"$PYTHON" -m pip install -r requirements.txt

echo "[setup] .env"
if [ ! -f ".env" ]; then
  cp .env.example .env
  echo "  -> created .env (please edit Reddit credentials before live mode)"
fi

echo "[setup] Docker check"
if ! command -v docker >/dev/null; then
  echo "  WARNING: docker not found on PATH. Install Docker Desktop." >&2
fi

echo "[setup] OK"
