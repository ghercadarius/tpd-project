"""Create Kafka topics declared in config/topics.yml (idempotent).

Usage:
    python scripts/init_kafka.py            # create missing topics
    python scripts/init_kafka.py --check    # exit 1 if any topic is missing
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import yaml
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

LOG = logging.getLogger("init_kafka")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv()

    p = argparse.ArgumentParser()
    p.add_argument("--config", type=Path, default=Path("config/topics.yml"))
    p.add_argument("--check", action="store_true", help="Exit 1 if topics are missing.")
    args = p.parse_args()

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    LOG.info("connecting to Kafka at %s", bootstrap)
    admin = AdminClient({"bootstrap.servers": bootstrap})

    declared: list[dict] = yaml.safe_load(args.config.read_text())["topics"]
    existing = admin.list_topics(timeout=15).topics

    errors = 0
    to_create: list[NewTopic] = []

    for t in declared:
        name = t["name"]
        if name in existing:
            current = len(existing[name].partitions)
            if current != t["partitions"]:
                LOG.warning("partition drift: %s has %d, expected %d", name, current, t["partitions"])
                errors += 1
            else:
                LOG.info("ok  %s (%d partitions)", name, current)
        else:
            if args.check:
                LOG.error("missing topic: %s", name)
                errors += 1
            else:
                to_create.append(NewTopic(
                    topic=name,
                    num_partitions=t["partitions"],
                    replication_factor=t.get("replication", 1),
                    config={k: str(v) for k, v in t.get("config", {}).items()},
                ))

    if to_create:
        futures = admin.create_topics(to_create)
        for name, fut in futures.items():
            try:
                fut.result()
                LOG.info("created %s", name)
            except Exception as exc:
                LOG.error("failed to create %s: %s", name, exc)
                errors += 1

    if errors:
        LOG.error("%d error(s)", errors)
        return 1
    LOG.info("all topics OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
