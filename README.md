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

## Quickstart (x86 Linux)

**Prerequisites:** Docker, Python 3.10 or 3.11 (PyFlink requires ≤ 3.11), Java 11+.

```bash
# 1. Create a virtual environment with the right Python version
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Copy and fill in the environment file
cp .env.example .env      # edit if needed; defaults work for local Docker

# 3. Bring up Kafka + Postgres
docker compose -f infra/docker-compose.yml up -d --wait

# 4. Create Kafka topics
python scripts/init_kafka.py

# 5. Start the Flink job (local mini-cluster inside the Python process)
python -m flink_jobs.brand_crisis_job &

# 6. Start the Postgres sink consumer
python -m dashboard.sink_consumer &

# 7. Replay your .zst dump into Kafka
python -m producers.pushshift_replay --file /path/to/dump.zst --rate max

# 8. Start the API + dashboard
uvicorn dashboard.api:app --port 8000 &
streamlit run dashboard/app.py --server.port 8501
# -> http://localhost:8501
```

One-shot orchestration (steps 3–8 in one command):
```bash
FILE=/path/to/dump.zst bash scripts/run_all.sh
```

### Optional: ONNX sentiment model (better accuracy)

By default the pipeline uses VADER (no download needed).
To upgrade to the pretrained Twitter-RoBERTa model:
```bash
pip install torch transformers optimum
python scripts/export_model.py          # downloads + exports ~500 MB model
# then set in .env:
SENTIMENT_BACKEND=onnx
```

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
