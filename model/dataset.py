"""Build a weak-labeled training dataset for Reddit sentiment.

Strategy
--------
1. Load a public sentiment corpus (HuggingFace `tweet_eval/sentiment` by default)
   for high-quality 3-class labels (neg / neu / pos).
2. Optionally mix in Reddit-style text from a Pushshift NDJSON dump, weak-labeled
   with VADER, to bridge the domain gap.
3. Write train/val/test parquet files plus a small stats report under data/train/.
"""
from __future__ import annotations

import argparse
import json
import logging
import random
from pathlib import Path
from typing import Iterable, Iterator

import pandas as pd

LOG = logging.getLogger("dataset")
LABELS = ["neg", "neu", "pos"]
LABEL2ID = {l: i for i, l in enumerate(LABELS)}


def _iter_pushshift(path: Path) -> Iterator[dict]:
    import zstandard as zstd

    if path.suffix == ".zst":
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


def _vader_label(text: str, sia) -> str:
    s = sia.polarity_scores(text)["compound"]
    if s <= -0.3:
        return "neg"
    if s >= 0.3:
        return "pos"
    return "neu"


def load_public(limit: int | None) -> pd.DataFrame:
    from datasets import load_dataset

    LOG.info("Loading tweet_eval/sentiment ...")
    ds = load_dataset("tweet_eval", "sentiment")
    rows: list[dict] = []
    # tweet_eval label order: 0=neg, 1=neu, 2=pos
    inv = {0: "neg", 1: "neu", 2: "pos"}
    for split in ("train", "validation", "test"):
        for ex in ds[split]:
            rows.append({"text": ex["text"], "label": inv[ex["label"]], "source": "public"})
    df = pd.DataFrame(rows)
    if limit:
        df = df.sample(min(limit, len(df)), random_state=42).reset_index(drop=True)
    return df


def load_reddit_weak(dump: Path, limit: int) -> pd.DataFrame:
    from nltk.sentiment import SentimentIntensityAnalyzer
    import nltk

    nltk.download("vader_lexicon", quiet=True)
    sia = SentimentIntensityAnalyzer()

    rows: list[dict] = []
    for i, rec in enumerate(_iter_pushshift(dump)):
        text = (rec.get("body") or rec.get("selftext") or rec.get("title") or "").strip()
        if not text or len(text) < 8:
            continue
        rows.append({"text": text[:512], "label": _vader_label(text, sia), "source": "reddit_weak"})
        if len(rows) >= limit:
            break
    LOG.info("Collected %d weak-labeled Reddit rows", len(rows))
    return pd.DataFrame(rows)


def split_and_save(df: pd.DataFrame, out_dir: Path) -> dict:
    df = df.sample(frac=1.0, random_state=42).reset_index(drop=True)
    n = len(df)
    n_train = int(n * 0.8)
    n_val = int(n * 0.1)
    parts = {
        "train": df.iloc[:n_train],
        "val":   df.iloc[n_train:n_train + n_val],
        "test":  df.iloc[n_train + n_val:],
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    stats: dict = {"total": n, "splits": {}}
    for split, part in parts.items():
        path = out_dir / f"{split}.parquet"
        part.to_parquet(path, index=False)
        stats["splits"][split] = {
            "rows": len(part),
            "label_counts": part["label"].value_counts().to_dict(),
        }
    (out_dir / "stats.json").write_text(json.dumps(stats, indent=2))
    return stats


def build(public_limit: int | None, reddit_dump: Path | None,
          reddit_limit: int, out_dir: Path) -> dict:
    parts: list[pd.DataFrame] = [load_public(public_limit)]
    if reddit_dump and reddit_dump.exists():
        parts.append(load_reddit_weak(reddit_dump, reddit_limit))
    df = pd.concat(parts, ignore_index=True)
    df = df.dropna(subset=["text"]).drop_duplicates(subset=["text"])
    return split_and_save(df, out_dir)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--public-limit", type=int, default=None,
                   help="Cap on public-corpus rows (default: use all).")
    p.add_argument("--reddit-dump", type=Path, default=None,
                   help="Optional Pushshift NDJSON / .zst file for weak labeling.")
    p.add_argument("--reddit-limit", type=int, default=20000)
    p.add_argument("--out-dir", type=Path, default=Path("data/train"))
    args = p.parse_args()
    random.seed(42)
    stats = build(args.public_limit, args.reddit_dump, args.reddit_limit, args.out_dir)
    LOG.info("Stats: %s", json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
