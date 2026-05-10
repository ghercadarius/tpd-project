"""Download a Reddit Pushshift dump for a given month and subreddit list.

Pushshift now requires registration; this script accepts a direct URL
(e.g., academictorrents/HF mirror) so it can be reused with whatever access
the user has. Resumable via HTTP Range when the server allows it.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import requests

LOG = logging.getLogger("download_pushshift")


def download(url: str, dest: Path, chunk_size: int = 2**20) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    pos = dest.stat().st_size if dest.exists() else 0
    headers = {"Range": f"bytes={pos}-"} if pos else {}
    LOG.info("GET %s (resume from %d)", url, pos)
    with requests.get(url, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0)) + pos
        mode = "ab" if pos else "wb"
        with dest.open(mode) as fh:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    fh.write(chunk)
                    pos += len(chunk)
        LOG.info("wrote %d bytes (%.1f%%)", pos, 100 * pos / total if total else 0)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--url", required=True, help="Direct URL to the Pushshift dump (.ndjson or .zst)")
    p.add_argument("--out", type=Path, default=Path("data/pushshift/dump.ndjson.zst"))
    args = p.parse_args()
    download(args.url, args.out)


if __name__ == "__main__":
    main()
