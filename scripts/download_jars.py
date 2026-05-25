"""Download the Flink-Kafka connector JAR required for local execution.

The flink-sql-connector-kafka fat-jar bundles kafka-clients so it is the only
file needed for local / mini-cluster mode.

Usage:
    python scripts/download_jars.py
"""
from __future__ import annotations

import logging
import urllib.request
from pathlib import Path

LOG = logging.getLogger("download_jars")

# flink-sql-connector-kafka is the uber-jar (includes kafka-clients).
# Version 3.3.0-1.19 is compatible with Flink 1.19.x.
JARS = [
    {
        "name": "flink-sql-connector-kafka-3.3.0-1.19.jar",
        "url": (
            "https://repo1.maven.org/maven2/org/apache/flink/"
            "flink-sql-connector-kafka/3.3.0-1.19/"
            "flink-sql-connector-kafka-3.3.0-1.19.jar"
        ),
    },
]

JAR_DIR = Path(__file__).parent.parent / "flink_jobs" / "jars"


def _download(url: str, dest: Path) -> None:
    LOG.info("downloading %s …", dest.name)
    with urllib.request.urlopen(url) as resp, dest.open("wb") as fh:
        total = 0
        while chunk := resp.read(1 << 20):
            fh.write(chunk)
            total += len(chunk)
    LOG.info("saved %s (%.1f MB)", dest, total / 1e6)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    JAR_DIR.mkdir(parents=True, exist_ok=True)
    for jar in JARS:
        dest = JAR_DIR / jar["name"]
        if dest.exists():
            LOG.info("already present: %s", dest.name)
            continue
        _download(jar["url"], dest)
    LOG.info("all JARs ready in %s", JAR_DIR)


if __name__ == "__main__":
    main()
