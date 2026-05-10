"""Lightweight ONNX-Runtime sentiment scorer used by the Flink UDF and tests.

Designed to be cheap to instantiate per-TaskManager and thread-safe per-instance.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np

LABELS = ["neg", "neu", "pos"]


@dataclass
class Score:
    label: str
    neg_prob: float
    confidence: float


def _softmax(x: np.ndarray) -> np.ndarray:
    x = x - x.max(axis=-1, keepdims=True)
    e = np.exp(x)
    return e / e.sum(axis=-1, keepdims=True)


class SentimentScorer:
    """Wraps an ONNX sentiment classifier and a HF tokenizer."""

    def __init__(self,
                 onnx_path: str | os.PathLike | None = None,
                 tokenizer_dir: str | os.PathLike | None = None,
                 max_len: int = 128) -> None:
        import onnxruntime as ort
        from transformers import AutoTokenizer

        onnx_path = Path(onnx_path or os.environ.get("ONNX_MODEL", "model/artifacts/sentiment.int8.onnx"))
        tokenizer_dir = Path(tokenizer_dir or os.environ.get("TOKENIZER_DIR", "model/artifacts/tokenizer"))

        if not onnx_path.exists():
            raise FileNotFoundError(f"ONNX model not found: {onnx_path}")
        if not tokenizer_dir.exists():
            raise FileNotFoundError(f"Tokenizer dir not found: {tokenizer_dir}")

        sess_opts = ort.SessionOptions()
        sess_opts.intra_op_num_threads = int(os.environ.get("ORT_THREADS", "1"))
        self._session = ort.InferenceSession(str(onnx_path), sess_options=sess_opts,
                                             providers=["CPUExecutionProvider"])
        self._tokenizer = AutoTokenizer.from_pretrained(tokenizer_dir)
        self._max_len = max_len
        self._input_names = {i.name for i in self._session.get_inputs()}

    def score(self, text: str) -> Score:
        return self.score_batch([text])[0]

    def score_batch(self, texts: Iterable[str]) -> list[Score]:
        texts = [t or "" for t in texts]
        enc = self._tokenizer(
            texts, padding=True, truncation=True, max_length=self._max_len, return_tensors="np"
        )
        feeds = {k: v for k, v in enc.items() if k in self._input_names}
        logits = self._session.run(None, feeds)[0]
        probs = _softmax(logits)
        out: list[Score] = []
        for row in probs:
            idx = int(row.argmax())
            out.append(Score(label=LABELS[idx], neg_prob=float(row[0]), confidence=float(row[idx])))
        return out


# Module-level singleton used by the Flink UDF (lazy, per-process).
_SCORER: SentimentScorer | None = None


def get_scorer() -> SentimentScorer:
    global _SCORER
    if _SCORER is None:
        _SCORER = SentimentScorer()
    return _SCORER
