"""FastAPI backend for the dashboard.

Endpoints
---------
GET /brands                        list known brands (from Postgres)
GET /aggregates?brand=&minutes=    time-series of 5-min aggregates
GET /alerts?active=true&limit=     recent alerts
GET /stream/alerts                 SSE stream tailing reddit.alerts
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from confluent_kafka import Consumer
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from sqlalchemy import create_engine, text
from sse_starlette.sse import EventSourceResponse

load_dotenv()
LOG = logging.getLogger("api")

ENGINE = create_engine(
    f"postgresql+psycopg://{os.environ.get('POSTGRES_USER','brand')}:"
    f"{os.environ.get('POSTGRES_PASSWORD','brand')}@"
    f"{os.environ.get('POSTGRES_HOST','localhost')}:"
    f"{os.environ.get('POSTGRES_PORT','5432')}/"
    f"{os.environ.get('POSTGRES_DB','brandcrisis')}",
    pool_pre_ping=True,
)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    yield


app = FastAPI(title="Brand Crisis Dashboard API", lifespan=_lifespan)


@app.get("/brands")
def brands() -> list[str]:
    with ENGINE.connect() as conn:
        rows = conn.execute(text("SELECT DISTINCT brand FROM aggregates_5m ORDER BY brand")).all()
    return [r[0] for r in rows]


@app.get("/aggregates")
def aggregates(brand: str, minutes: int = Query(120, ge=1, le=24 * 60)) -> list[dict]:
    cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=minutes)
    sql = text("""
        SELECT window_start, window_end, volume, neg_count, neg_ratio,
               avg_neg_prob, unique_authors, influencer_neg
          FROM aggregates_5m
         WHERE brand = :brand AND window_start >= :cutoff
         ORDER BY window_start ASC
    """)
    with ENGINE.connect() as conn:
        rows = conn.execute(sql, {"brand": brand, "cutoff": cutoff}).mappings().all()
    return [dict(r) for r in rows]


@app.get("/alerts")
def alerts(active: bool = False, limit: int = Query(50, ge=1, le=500)) -> list[dict]:
    sql = """
        SELECT id, brand, triggered_at, window_start, window_end,
               z_score, neg_ratio, volume, severity, sample_text
          FROM alerts
    """
    if active:
        sql += " WHERE triggered_at >= NOW() - INTERVAL '1 hour' "
    sql += " ORDER BY triggered_at DESC LIMIT :limit"
    with ENGINE.connect() as conn:
        rows = conn.execute(text(sql), {"limit": limit}).mappings().all()
    return [dict(r) for r in rows]


@app.get("/stream/alerts")
async def stream_alerts():
    """Server-Sent Events tailing the reddit.alerts Kafka topic."""
    consumer = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        "group.id": f"dashboard-sse-{os.getpid()}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([os.environ.get("TOPIC_ALERTS", "reddit.alerts")])

    async def _gen():
        try:
            while True:
                msg = await asyncio.get_event_loop().run_in_executor(None, consumer.poll, 1.0)
                if msg is None or msg.error():
                    yield {"event": "ping", "data": "{}"}
                    continue
                yield {"event": "alert", "data": msg.value().decode("utf-8")}
        finally:
            consumer.close()

    return EventSourceResponse(_gen())


@app.get("/health")
def health() -> dict:
    return {"ok": True}
