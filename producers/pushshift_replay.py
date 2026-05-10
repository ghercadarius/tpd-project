"""Replay a Pushshift NDJSON dump into Kafka, keyed by brand.

Modes:
    --rate realtime   sleep based on consecutive `created_utc` deltas
    --rate max        push as fast as possible (backfill / smoke tests)
    --rate <float>    multiplier (e.g. 10.0 = 10x faster than wall clock)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Iterator

from confluent_kafka import Producer
from dotenv import load_dotenv

from producers.common import load_brands, match_brand

LOG = logging.getLogger("pushshift_replay")


def iter_records(path: Path) -> Iterator[dict]:
    if path.suffix == ".zst":
        import zstandard as zstd

        with path.open("rb") as fh:
            dctx = zstd.ZstdDecompressor(max_window_size=2**31)
            with dctx.stream_reader(fh) as reader:
                buf = b""
                while True:
                    chunk = reader.read(2**20)
                    if not chunk:
                        break
                    buf += chunk
                    *lines, buf = buf.split(b"\n")
                    for line in lines:
                        if line.strip():
                            yield json.loads(line)
    else:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield json.loads(line)


def to_message(rec: dict, brand: str) -> tuple[str, dict]:
    is_comment = "body" in rec and "title" not in rec
    msg = {
        "id": rec.get("id") or rec.get("name"),
        "type": "comment" if is_comment else "submission",
        "subreddit": rec.get("subreddit", ""),
        "author": rec.get("author"),
        "brand": brand,
        "title": rec.get("title"),
        "body": rec.get("body") or rec.get("selftext") or "",
        "permalink": rec.get("permalink"),
        "score": int(rec.get("score") or 0),
        "parent_id": rec.get("parent_id"),
        "created_utc": float(rec.get("created_utc") or 0),
        "ingested_at": time.time(),
    }
    return msg["type"], msg


def _delivery(err, msg):
    if err is not None:
        LOG.warning("delivery failed: %s", err)


def _parse_rate(rate: str) -> float | None:
    if rate == "max":
        return None
    if rate == "realtime":
        return 1.0
    return float(rate)


def run(file: Path, rate: str, max_messages: int | None) -> None:
    load_dotenv()
    brands = load_brands()
    producer = Producer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        "client.id": os.environ.get("KAFKA_CLIENT_ID", "pushshift-replay"),
        "linger.ms": 10,
        "compression.type": "lz4",
    })
    topic_sub = os.environ.get("TOPIC_RAW_SUBMISSIONS", "reddit.raw.submissions")
    topic_com = os.environ.get("TOPIC_RAW_COMMENTS", "reddit.raw.comments")

    speed = _parse_rate(rate)
    sent = 0
    skipped = 0
    last_event_ts: float | None = None

    for rec in iter_records(file):
        text = " ".join(filter(None, [rec.get("title"), rec.get("selftext"), rec.get("body")]))
        brand = match_brand(text, brands)
        if brand is None:
            skipped += 1
            continue

        kind, msg = to_message(rec, brand)
        topic = topic_sub if kind == "submission" else topic_com

        if speed is not None and last_event_ts is not None:
            delta = msg["created_utc"] - last_event_ts
            if delta > 0:
                time.sleep(delta / speed)
        last_event_ts = msg["created_utc"]

        producer.produce(
            topic=topic,
            key=brand.encode("utf-8"),
            value=json.dumps(msg).encode("utf-8"),
            on_delivery=_delivery,
        )
        sent += 1
        if sent % 1000 == 0:
            producer.poll(0)
            LOG.info("sent=%d skipped=%d", sent, skipped)
        if max_messages and sent >= max_messages:
            break

    producer.flush(30)
    LOG.info("DONE sent=%d skipped=%d", sent, skipped)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--file", type=Path, required=True)
    p.add_argument("--rate", default="max",
                   help="'realtime', 'max', or a float multiplier (e.g. 10).")
    p.add_argument("--max", type=int, default=None, dest="max_messages")
    args = p.parse_args()
    run(args.file, args.rate, args.max_messages)


if __name__ == "__main__":
    main()
