"""Idempotent Kafka topic management driven by config/topics.yml.

Examples:
    python scripts/init_kafka.py            # create-if-missing
    python scripts/init_kafka.py --check    # exit non-zero on drift
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import yaml
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic
from dotenv import load_dotenv

LOG = logging.getLogger("init_kafka")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv()
    p = argparse.ArgumentParser()
    p.add_argument("--config", type=Path, default=Path("config/topics.yml"))
    p.add_argument("--check", action="store_true")
    args = p.parse_args()

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    admin = AdminClient({"bootstrap.servers": bootstrap})
    declared = yaml.safe_load(args.config.read_text())["topics"]
    existing = admin.list_topics(timeout=10).topics

    drift = 0
    to_create: list[NewTopic] = []
    for t in declared:
        name = t["name"]
        if name not in existing:
            LOG.info("missing -> create %s (partitions=%d)", name, t["partitions"])
            if args.check:
                drift += 1
            else:
                to_create.append(NewTopic(
                    topic=name,
                    num_partitions=t["partitions"],
                    replication_factor=t.get("replication", 1),
                    config=t.get("config", {}),
                ))
        else:
            current_parts = len(existing[name].partitions)
            if current_parts != t["partitions"]:
                LOG.warning("PARTITION DRIFT: %s has %d, expected %d",
                            name, current_parts, t["partitions"])
                drift += 1

    if to_create:
        for name, fut in admin.create_topics(to_create).items():
            try:
                fut.result()
                LOG.info("created %s", name)
            except Exception as exc:
                LOG.error("failed to create %s: %s", name, exc)
                drift += 1

    if args.check and drift:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
