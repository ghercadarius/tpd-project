"""Inject a synthetic burst of negative messages for one brand and assert that
an alert appears on reddit.alerts within a timeout. Used by CI and as a
sanity check post-deploy.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

LOG = logging.getLogger("smoke")
NEGATIVE_TEXTS = [
    "Absolutely terrible product, I want a refund right now.",
    "This brand is a scam, never buying again.",
    "Worst customer service I have ever experienced.",
    "Total disaster — totally broken on day one.",
    "Disgusting behavior from this company, boycott them.",
]


def run(brand: str, count: int, timeout: int) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv()

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    topic_in = os.environ.get("TOPIC_RAW_COMMENTS", "reddit.raw.comments")
    topic_alert = os.environ.get("TOPIC_ALERTS", "reddit.alerts")

    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 10})
    now = time.time()
    LOG.info("injecting %d negative comments for brand=%s", count, brand)
    for i in range(count):
        msg = {
            "id": f"smoke-{uuid.uuid4()}",
            "type": "comment",
            "subreddit": "smoketest",
            "author": f"smoke_user_{i % 50}",
            "brand": brand,
            "title": None,
            "body": NEGATIVE_TEXTS[i % len(NEGATIVE_TEXTS)],
            "permalink": None,
            "score": 5,
            "parent_id": None,
            "created_utc": now + i * 0.1,
            "ingested_at": now + i * 0.1,
        }
        producer.produce(topic_in, key=brand.encode(), value=json.dumps(msg).encode())
    producer.flush(10)

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"smoke-{uuid.uuid4()}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic_alert])

    deadline = time.time() + timeout
    LOG.info("waiting up to %ds for alert on %s ...", timeout, topic_alert)
    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            payload = json.loads(msg.value())
            if payload.get("brand") == brand:
                LOG.info("ALERT received: %s", payload)
                return 0
    finally:
        consumer.close()
    LOG.error("no alert received within %ds", timeout)
    return 1


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--brand", default="acme")
    p.add_argument("--count", type=int, default=200)
    p.add_argument("--timeout", type=int, default=180)
    args = p.parse_args()
    sys.exit(run(args.brand, args.count, args.timeout))


if __name__ == "__main__":
    main()
