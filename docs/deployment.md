# Deployment

## Local (Docker Compose)

The reference deployment runs entirely on a single host via
[infra/docker-compose.yml](../infra/docker-compose.yml):

| Service | Port | Notes |
|---|---|---|
| Kafka (KRaft) | 19092 (host) / 9092 (internal) | Auto-create disabled; topics are created by `init_kafka.py`. |
| Kafka UI | 8080 | Provectus Kafka UI. |
| Flink JobManager | 8081 | Web UI; checkpoints volume mounted. |
| Flink TaskManagers | — | 2 replicas × 4 slots = 8 parallel slots. |
| Postgres | 5432 | Init SQL creates `aggregates_5m`, `alerts`. |

Start: `./scripts/start_infra.ps1` (or `bash scripts/start_infra.sh`).
Tear down: `bash scripts/teardown.sh`.

## Configuration sources

| Source | Used by |
|---|---|
| `.env` (copy of `.env.example`) | Producers, sink consumer, API, scripts. |
| `config/brands.yml` | Producers (keyword filtering). |
| `config/topics.yml` | `init_kafka.py`. |
| `config/detection.yml` | Flink job (window + spike thresholds). |
| `model/artifacts/` | Flink TaskManagers (mounted at `/opt/flink/usrlib/model_artifacts`). |

## Reddit OAuth2

1. Visit <https://www.reddit.com/prefs/apps> and create a `script` app.
2. Fill in `.env`:
   ```
   REDDIT_CLIENT_ID=...
   REDDIT_CLIENT_SECRET=...
   REDDIT_USER_AGENT=brand-crisis-monitor/0.1 by your_username
   REDDIT_USERNAME=...
   REDDIT_PASSWORD=...
   ```
3. The PRAW producer authenticates on first message; failures are logged but
   do not crash the stream (retried by PRAW).

## Scaling

- **Kafka**: increase `partitions` for raw topics; rebalance Flink parallelism
  to match.
- **Flink parallelism**: `FLINK_PARALLELISM` env var or `-p` to `flink run`.
  Up to `partitions × keyspace` is useful; beyond that the spike detector
  becomes idle.
- **GPU vs CPU**: training requires CUDA; serving runs on CPU inside the
  Flink TaskManagers (int8 ONNX). No GPU is needed in production.
- **Memory**: TaskManager process size scales with the loaded ONNX model
  (~70MB for DistilBERT int8) plus RocksDB state.

## Secrets layout
- `.env` is gitignored; do not commit. Use a secret manager in production.
- Postgres credentials are dev-only; rotate before any external exposure.
