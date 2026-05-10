#!/usr/bin/env bash
# Tear down infra and remove transient state. Keeps trained model artifacts.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$HERE"

docker compose -f infra/docker-compose.yml down -v
rm -rf checkpoints data/tmp
echo "[teardown] OK"
