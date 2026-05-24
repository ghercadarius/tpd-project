# Reddit Brand-Crisis Streaming Pipeline

A lightweight real-time pipeline that ingests Reddit posts/comments, scores per-message
sentiment with a pretrained transformer served as quantized ONNX, detects negative
spikes per brand using stateful Flink operators, and exposes trends + alerts on a
Streamlit/FastAPI dashboard.

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

## Quickstart (5 minutes, macOS replay mode)

Install Docker Desktop and Python 3 first. The bash scripts auto-detect `.venv/bin/python`
when available and otherwise fall back to `python3`.

```bash
# 1. Install Python deps & bootstrap .env
bash scripts/setup.sh

# 2. Bring up Kafka + Flink + Postgres and create topics
bash scripts/start_infra.sh

# 3. Materialize the pretrained sentiment artifacts
python3 scripts/export_model.py

# 3b. (Optional) Rebuild the experimental dataset / eval gate
python3 scripts/prepare_dataset.py
python3 scripts/eval_model.py

# 4. Start producers in replay mode using a small sample dump
bash scripts/start_producers.sh --mode replay --file data/pushshift/sample.ndjson

# 5. Submit the Flink job
bash scripts/submit_flink_job.sh

# 6. Open the dashboard
bash scripts/start_dashboard.sh
# -> http://localhost:8501
```

For a one-shot orchestration, use `bash scripts/run_all.sh`.

## Repository layout

```
config/         brands.yml, topics.yml, message schemas
producers/      Pushshift replay + live Reddit (PRAW) producers
model/          optional dataset prep, pretrained ONNX export, inference wrapper
flink_jobs/     PyFlink DataStream job + custom operators
dashboard/      FastAPI backend + Streamlit frontend
infra/          docker-compose for Kafka, Flink, Postgres
scripts/        setup, artifact export, deployment, smoke-test scripts
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
