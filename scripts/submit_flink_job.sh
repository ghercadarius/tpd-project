#!/usr/bin/env bash
# Submit the PyFlink job to the local cluster.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$HERE"

PARALLELISM="${FLINK_PARALLELISM:-4}"
JOBMANAGER="${FLINK_JOBMANAGER:-localhost:8081}"

# Run inside the JobManager container so Python deps and Kafka connector are present.
docker compose -f infra/docker-compose.yml exec -T jobmanager bash -lc "
  pip install --quiet pyflink-kafka-connector || true;
  flink run \
    -d \
    -p ${PARALLELISM} \
    -m ${JOBMANAGER} \
    --pyModule flink_jobs.brand_crisis_job \
    --pyFiles /opt/flink/usrlib/flink_jobs,/opt/flink/usrlib/model_artifacts,/opt/flink/usrlib/config
"
echo "[flink] job submitted; check ${JOBMANAGER}"
