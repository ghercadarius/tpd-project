# Reddit Brand-Crisis Streaming Pipeline

A lightweight real-time pipeline that ingests Reddit posts/comments, scores per-message
sentiment with a locally GPU-trained transformer (served as quantized ONNX), detects
negative spikes per brand using stateful Flink operators, and exposes trends + alerts on
a Streamlit/FastAPI dashboard.

> Sources: Reddit Pushshift (historical replay) and the official Reddit API (live).
> Engines: Apache Kafka + Apache Flink (PyFlink).

## Architecture

```
[Pushshift dump]──┐                                          ┌──> reddit.alerts
                  ├─> Producers ─> Kafka (key=brand) ─> PyFlink Job ─> reddit.scored
[Reddit API ]─────┘                                          └──> reddit.aggregates
                                                                       │
                                                                       ▼
                                                                  Postgres
                                                                       │
                                                                       ▼
                                                            FastAPI + Streamlit
```

See [docs/architecture.md](docs/architecture.md) for the full design.

## Quickstart (5 minutes, replay mode)

```powershell
# 1. Install Python deps & bootstrap .env
./scripts/setup.ps1

# 2. Bring up Kafka + Flink + Postgres and create topics
./scripts/start_infra.ps1

# 3. (Optional) Train a sentiment model on a local GPU
python scripts/prepare_dataset.py
python scripts/train_model.py --epochs 3
python scripts/export_model.py
python scripts/eval_model.py

# 4. Start producers in replay mode using a small sample dump
bash scripts/start_producers.sh --mode replay --file data/pushshift/sample.ndjson

# 5. Submit the Flink job
./scripts/submit_flink_job.ps1

# 6. Open the dashboard
bash scripts/start_dashboard.sh
# -> http://localhost:8501
```

For a one-shot orchestration, use `./scripts/run_all.ps1` (or `bash scripts/run_all.sh`).

## Repository layout

```
config/         brands.yml, topics.yml, message schemas
producers/      Pushshift replay + live Reddit (PRAW) producers
model/          dataset, training, ONNX export, inference wrapper
flink_jobs/     PyFlink DataStream job + custom operators
dashboard/      FastAPI backend + Streamlit frontend
infra/          docker-compose for Kafka, Flink, Postgres
scripts/        setup, training, deployment, smoke-test scripts
docs/           architecture, model, runbook, deployment, etc.
tests/          unit tests
```

## Documentation index

- [Architecture](docs/architecture.md)
- [Data pipeline & schemas](docs/data_pipeline.md)
- [Sentiment model](docs/model.md)
- [Flink job & detection logic](docs/flink_job.md)
- [Dashboard](docs/dashboard.md)
- [Deployment](docs/deployment.md)
- [Operations runbook](docs/runbook.md)
