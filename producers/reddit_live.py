"""Live Reddit producer using PRAW streams.

Streams /r/all submissions and comments, filters by brand keywords from
config/brands.yml, and pushes to Kafka with key=brand.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time

import praw
from confluent_kafka import Producer
from dotenv import load_dotenv

from producers.common import load_brands, match_brand

LOG = logging.getLogger("reddit_live")


def _make_reddit() -> praw.Reddit:
    return praw.Reddit(
        client_id=os.environ["REDDIT_CLIENT_ID"],
        client_secret=os.environ["REDDIT_CLIENT_SECRET"],
        username=os.environ.get("REDDIT_USERNAME") or None,
        password=os.environ.get("REDDIT_PASSWORD") or None,
        user_agent=os.environ.get("REDDIT_USER_AGENT", "brand-crisis-monitor/0.1"),
    )


def _produce(producer: Producer, topic: str, key: str, msg: dict) -> None:
    producer.produce(topic=topic, key=key.encode("utf-8"),
                     value=json.dumps(msg).encode("utf-8"))


def stream(kind: str, subreddits: str = "all") -> None:
    """Run a single stream type ('submissions' or 'comments')."""
    load_dotenv()
    brands = load_brands()
    reddit = _make_reddit()
    sub = reddit.subreddit(subreddits)

    producer = Producer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        "client.id": f"reddit-live-{kind}",
        "linger.ms": 50,
        "compression.type": "lz4",
    })
    topic = (os.environ.get("TOPIC_RAW_SUBMISSIONS", "reddit.raw.submissions")
             if kind == "submissions"
             else os.environ.get("TOPIC_RAW_COMMENTS", "reddit.raw.comments"))

    iterator = sub.stream.submissions(skip_existing=True) if kind == "submissions" \
        else sub.stream.comments(skip_existing=True)

    LOG.info("Streaming %s from r/%s -> %s", kind, subreddits, topic)
    for item in iterator:
        try:
            if kind == "submissions":
                text = f"{item.title or ''} {item.selftext or ''}"
                msg = {
                    "id": item.id,
                    "type": "submission",
                    "subreddit": str(item.subreddit),
                    "author": str(item.author) if item.author else None,
                    "title": item.title,
                    "body": item.selftext or "",
                    "permalink": item.permalink,
                    "score": int(getattr(item, "score", 0) or 0),
                    "parent_id": None,
                    "created_utc": float(item.created_utc),
                }
            else:
                text = item.body or ""
                msg = {
                    "id": item.id,
                    "type": "comment",
                    "subreddit": str(item.subreddit),
                    "author": str(item.author) if item.author else None,
                    "title": None,
                    "body": item.body or "",
                    "permalink": item.permalink,
                    "score": int(getattr(item, "score", 0) or 0),
                    "parent_id": item.parent_id,
                    "created_utc": float(item.created_utc),
                }
            brand = match_brand(text, brands)
            if not brand:
                continue
            msg["brand"] = brand
            msg["ingested_at"] = time.time()
            _produce(producer, topic, brand, msg)
            producer.poll(0)
        except Exception as exc:  # don't kill the stream on a single bad record
            LOG.exception("error processing item: %s", exc)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--kind", choices=["submissions", "comments"], required=True)
    p.add_argument("--subreddits", default="all")
    args = p.parse_args()
    stream(args.kind, args.subreddits)


if __name__ == "__main__":
    main()
