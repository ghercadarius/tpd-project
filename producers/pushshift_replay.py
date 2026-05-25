"""Replay a downloaded Reddit dataset into Kafka, keyed by brand.

Modes:
    --rate realtime   sleep proportional to consecutive created_utc deltas
    --rate max        push as fast as possible (default; good for smoke tests)
    --rate <float>    speed multiplier (e.g. 10.0 = 10× faster than wall-clock)

The input file can be a JSONL / NDJSON dump, optionally compressed with .zst,
or a CSV export with common Reddit columns.
"""
from __future__ import annotations

import argparse
import csv
import io
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


# ---------------------------------------------------------------------------
# Record parsing helpers
# ---------------------------------------------------------------------------

def _first(rec: dict, *keys: str, default=None):
    for key in keys:
        v = rec.get(key)
        if v not in (None, ""):
            return v
    return default


def _float(value, default: float = 0.0) -> float:
    try:
        return float(value) if value not in (None, "") else default
    except (TypeError, ValueError):
        return default


def _int(value, default: int = 0) -> int:
    try:
        return int(float(value)) if value not in (None, "") else default
    except (TypeError, ValueError):
        return default


def iter_records(path: Path) -> Iterator[dict]:
    """Yield parsed dicts from a .zst-compressed NDJSON or plain JSONL or CSV."""
    suffixes = [s.lower() for s in path.suffixes]

    if suffixes[-1:] == [".zst"]:
        import zstandard as zstd
        dctx = zstd.ZstdDecompressor()
        with path.open("rb") as fh, dctx.stream_reader(fh, closefd=False) as reader:
            text_stream = io.TextIOWrapper(reader, encoding="utf-8", errors="replace")
            for line in text_stream:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        pass

    elif suffixes[-1:] == [".csv"]:
        with path.open("r", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                if any(row.values()):
                    yield row

    else:
        # Plain JSONL / NDJSON
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        pass


def to_message(rec: dict, brand: str) -> tuple[str, dict]:
    """Normalise a raw record into the canonical message schema."""
    # Determine type from an explicit field or by presence of title vs body.
    kind = str(_first(rec, "type", "kind", default="")).strip().lower()
    if kind in {"comment", "submission"}:
        is_comment = kind == "comment"
    else:
        has_body = bool(_first(rec, "body"))
        has_title = bool(_first(rec, "title"))
        is_comment = has_body and not has_title

    msg = {
        "id": str(_first(rec, "id", "name", default="")),
        "type": "comment" if is_comment else "submission",
        "subreddit": str(_first(rec, "subreddit", default="unknown")),
        "author": _first(rec, "author"),
        "brand": brand,
        "title": _first(rec, "title"),
        "body": str(_first(rec, "body", "selftext", "text", default="") or ""),
        "permalink": _first(rec, "permalink"),
        "score": _int(_first(rec, "score", default=0)),
        "parent_id": _first(rec, "parent_id"),
        "created_utc": _float(_first(rec, "created_utc", "created", "timestamp", default=0.0)),
        "ingested_at": time.time(),
    }
    return msg["type"], msg


def _delivery_report(err, msg):
    if err is not None:
        LOG.warning("delivery failed topic=%s: %s", msg.topic(), err)


def _parse_rate(rate: str) -> float | None:
    """Return seconds-per-event-second, or None for max speed."""
    if rate == "max":
        return None
    if rate == "realtime":
        return 1.0
    return float(rate)


def run(file: Path, rate: str, max_messages: int | None) -> None:
    load_dotenv()
    brands = load_brands()
    if not brands:
        raise SystemExit("No brands configured — check config/brands.yml")

    producer = Producer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        "client.id": os.environ.get("KAFKA_CLIENT_ID", "pushshift-replay"),
        "linger.ms": 20,
        "compression.type": "lz4",
        "acks": "1",
    })

    topic_sub = os.environ.get("TOPIC_RAW_SUBMISSIONS", "reddit.raw.submissions")
    topic_com = os.environ.get("TOPIC_RAW_COMMENTS", "reddit.raw.comments")
    speed = _parse_rate(rate)

    sent = 0
    skipped = 0
    last_event_ts: float | None = None

    LOG.info("starting replay: file=%s rate=%s", file, rate)

    for rec in iter_records(file):
        text = " ".join(filter(None, [
            rec.get("title"), rec.get("selftext"), rec.get("body"),
        ]))
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
            key=brand.encode(),
            value=json.dumps(msg).encode(),
            on_delivery=_delivery_report,
        )
        sent += 1
        if sent % 500 == 0:
            producer.poll(0)
            LOG.info("sent=%d  skipped=%d", sent, skipped)
        if max_messages and sent >= max_messages:
            LOG.info("reached --max %d, stopping", max_messages)
            break

    producer.flush(30)
    LOG.info("DONE  sent=%d  skipped=%d", sent, skipped)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s  %(message)s",
    )
    p = argparse.ArgumentParser(description="Replay a Reddit dump into Kafka.")
    p.add_argument("--file", type=Path, required=True,
                   help="Path to JSONL, JSONL.zst, or CSV file.")
    p.add_argument("--rate", default="max",
                   help="'realtime', 'max', or a float speed multiplier.")
    p.add_argument("--max", type=int, default=None, dest="max_messages",
                   help="Stop after this many messages (default: all).")
    args = p.parse_args()
    run(args.file, args.rate, args.max_messages)


if __name__ == "__main__":
    main()
