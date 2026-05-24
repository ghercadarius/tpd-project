"""Compatibility wrapper for pretrained sentiment model artifacts.

The repo no longer fine-tunes a local sentiment model by default. This module
now materializes a pretrained checkpoint into the ONNX/tokenizer layout used by
the Flink runtime.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from model.export_onnx import DEFAULT_MODEL_ID, export

LOG = logging.getLogger("train")


def prepare(args: argparse.Namespace) -> None:
    LOG.info("Materializing pretrained sentiment artifacts from %s", args.model_id)
    export(args.model_id, args.out_dir)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--model-id", default=DEFAULT_MODEL_ID,
                   help="Hugging Face checkpoint to materialize.")
    p.add_argument("--out-dir", type=Path, default=Path("model/artifacts"))
    prepare(p.parse_args())


if __name__ == "__main__":
    main()
