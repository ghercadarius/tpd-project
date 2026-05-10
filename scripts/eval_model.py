"""Evaluate the exported ONNX model on the held-out test split.

Fails (non-zero exit) when accuracy regresses below the baseline stored at
model/artifacts/baseline.json (auto-created on first successful run).
Used as a deploy gate.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd
from sklearn.metrics import accuracy_score, f1_score

from model.inference import SentimentScorer

LOG = logging.getLogger("eval_model")


def evaluate(test_path: Path, baseline_path: Path, tolerance: float) -> int:
    df = pd.read_parquet(test_path)
    scorer = SentimentScorer()

    preds = [scorer.score(t).label for t in df["text"].tolist()]
    metrics = {
        "accuracy": accuracy_score(df["label"], preds),
        "f1_macro": f1_score(df["label"], preds, average="macro"),
        "n": len(df),
    }
    LOG.info("metrics=%s", metrics)

    if not baseline_path.exists():
        baseline_path.write_text(json.dumps(metrics, indent=2))
        LOG.info("Saved new baseline -> %s", baseline_path)
        return 0

    baseline = json.loads(baseline_path.read_text())
    delta = metrics["f1_macro"] - baseline["f1_macro"]
    if delta < -tolerance:
        LOG.error("REGRESSION: f1_macro %.4f < baseline %.4f - %.4f",
                  metrics["f1_macro"], baseline["f1_macro"], tolerance)
        return 2
    if delta > 0:
        baseline_path.write_text(json.dumps(metrics, indent=2))
        LOG.info("New baseline (improved by %.4f)", delta)
    return 0


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--test", type=Path, default=Path("data/train/test.parquet"))
    p.add_argument("--baseline", type=Path, default=Path("model/artifacts/baseline.json"))
    p.add_argument("--tolerance", type=float, default=0.01,
                   help="Maximum allowed f1_macro regression (default 1%%).")
    args = p.parse_args()
    sys.exit(evaluate(args.test, args.baseline, args.tolerance))


if __name__ == "__main__":
    main()
