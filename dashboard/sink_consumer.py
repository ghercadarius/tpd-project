"""Consume reddit.aggregates + reddit.alerts from Kafka and upsert into Postgres.

Runs as a sidecar so the Flink job stays focused on stream computation.
"""
from __future__ import annotations

import json
import logging
import os
import signal
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Consumer
from dotenv import load_dotenv

LOG = logging.getLogger("alerts_consumer")
RUNNING = True


def _ts(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def _upsert_aggregate(cur, agg: dict) -> None:
    cur.execute(
        """
        INSERT INTO aggregates_5m (
            brand, window_start, window_end, volume, neg_count,
            neg_ratio, avg_neg_prob, unique_authors, influencer_neg
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (brand, window_start) DO UPDATE SET
            window_end = EXCLUDED.window_end,
            volume = EXCLUDED.volume,
            neg_count = EXCLUDED.neg_count,
            neg_ratio = EXCLUDED.neg_ratio,
            avg_neg_prob = EXCLUDED.avg_neg_prob,
            unique_authors = EXCLUDED.unique_authors,
            influencer_neg = EXCLUDED.influencer_neg
        """,
        (
            agg["brand"], _ts(agg["window_start"]), _ts(agg["window_end"]),
            int(agg["volume"]), int(agg["neg_count"]),
            float(agg["neg_ratio"]), float(agg["avg_neg_prob"]),
            int(agg["unique_authors"]), float(agg.get("influencer_neg", 0.0)),
        ),
    )


def _insert_alert(cur, alert: dict) -> None:
    cur.execute(
        """
        INSERT INTO alerts (brand, triggered_at, window_start, window_end,
                            z_score, neg_ratio, volume, severity, sample_text)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            alert["brand"], _ts(alert["triggered_at"]),
            _ts(alert["window_start"]), _ts(alert["window_end"]),
            float(alert["z_score"]), float(alert["neg_ratio"]),
            int(alert["volume"]), alert["severity"], alert.get("sample_text"),
        ),
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv()

    consumer = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        "group.id": "dashboard-sink",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    topic_agg = os.environ.get("TOPIC_AGGREGATES", "reddit.aggregates")
    topic_alerts = os.environ.get("TOPIC_ALERTS", "reddit.alerts")
    consumer.subscribe([topic_agg, topic_alerts])

    dsn = (f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
           f"port={os.environ.get('POSTGRES_PORT', '5432')} "
           f"dbname={os.environ.get('POSTGRES_DB', 'brandcrisis')} "
           f"user={os.environ.get('POSTGRES_USER', 'brand')} "
           f"password={os.environ.get('POSTGRES_PASSWORD', 'brand')}")

    def _stop(*_):
        global RUNNING
        RUNNING = False
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        LOG.info("listening on %s, %s -> postgres", topic_agg, topic_alerts)
        while RUNNING:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                LOG.warning("kafka error: %s", msg.error())
                continue
            try:
                payload = json.loads(msg.value())
                if msg.topic() == topic_agg:
                    _upsert_aggregate(cur, payload)
                else:
                    _insert_alert(cur, payload)
            except Exception:
                LOG.exception("failed to write record")
    consumer.close()


if __name__ == "__main__":
    main()
