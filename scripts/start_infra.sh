#!/usr/bin/env bash
# Bring up infra and create Kafka topics.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
HERE="$(repo_root)"
cd "$HERE"

PYTHON="$(resolve_python "$HERE")"

echo "[infra] docker compose up -d"
docker compose -f infra/docker-compose.yml up -d

echo "[infra] waiting for Kafka health ..."
for i in $(seq 1 30); do
  if docker compose -f infra/docker-compose.yml exec -T kafka \
       kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    echo "  Kafka up."
    break
  fi
  sleep 2
done

echo "[infra] init topics"
"$PYTHON" scripts/init_kafka.py

echo "[infra] OK"
echo "  Kafka UI : http://localhost:8080"
echo "  Flink UI : http://localhost:8081"
echo "  Postgres : localhost:5432 (brand/brand)"
