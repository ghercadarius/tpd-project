#!/usr/bin/env bash
# Bootstrap a Python venv, install deps, and prepare the .env file.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$HERE"

echo "[setup] Python venv"
if [ ! -d ".venv" ]; then
  python -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate

echo "[setup] pip install"
python -m pip install --upgrade pip
pip install -r requirements.txt

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
