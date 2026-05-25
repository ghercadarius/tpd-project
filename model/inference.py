"""Sentiment scorer used by the Flink UDF.

Backend selection (SENTIMENT_BACKEND env var):
    vader  — NLTK VADER; no model download required (default)
    onnx   — ONNX Runtime with a HF tokenizer; run scripts/export_model.py first
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable


@dataclass
class Score:
    label: str       # "neg" | "neu" | "pos"
    neg_prob: float  # probability of negative sentiment [0, 1]
    confidence: float


# ---------------------------------------------------------------------------
# VADER backend
# ---------------------------------------------------------------------------

class _VaderScorer:
    def __init__(self) -> None:
        import nltk
        nltk.download("vader_lexicon", quiet=True)
        from nltk.sentiment import SentimentIntensityAnalyzer
        self._sia = SentimentIntensityAnalyzer()

    def score(self, text: str) -> Score:
        return self.score_batch([text])[0]

    def score_batch(self, texts: Iterable[str]) -> list[Score]:
        out: list[Score] = []
        for text in texts:
            s = self._sia.polarity_scores(text or "")
            compound = s["compound"]
            neg_raw = s["neg"]
            # Map compound [-1, 1] to a 3-class label.
            if compound <= -0.05:
                label = "neg"
            elif compound >= 0.05:
                label = "pos"
            else:
                label = "neu"
            # neg_prob: proportion of negative tokens, boosted by compound sign.
            neg_prob = neg_raw if compound < 0 else neg_raw * 0.3
            confidence = abs(compound) if compound != 0 else 1 - abs(compound)
            out.append(Score(label=label, neg_prob=float(neg_prob), confidence=float(confidence)))
        return out


# ---------------------------------------------------------------------------
# ONNX backend
# ---------------------------------------------------------------------------

import numpy as np

_LABELS = ["neg", "neu", "pos"]


def _softmax(x: np.ndarray) -> np.ndarray:
    x = x - x.max(axis=-1, keepdims=True)
    e = np.exp(x)
    return e / e.sum(axis=-1, keepdims=True)


class _OnnxScorer:
    def __init__(self,
                 onnx_path: str | os.PathLike | None = None,
                 tokenizer_dir: str | os.PathLike | None = None,
                 max_len: int = 128) -> None:
        import onnxruntime as ort
        from transformers import AutoTokenizer
        from pathlib import Path

        onnx_path = Path(onnx_path or os.environ.get("ONNX_MODEL", "model/artifacts/sentiment.int8.onnx"))
        tokenizer_dir = Path(tokenizer_dir or os.environ.get("TOKENIZER_DIR", "model/artifacts/tokenizer"))

        if not onnx_path.exists():
            raise FileNotFoundError(f"ONNX model not found: {onnx_path}")
        if not tokenizer_dir.exists():
            raise FileNotFoundError(f"Tokenizer dir not found: {tokenizer_dir}")

        opts = ort.SessionOptions()
        opts.intra_op_num_threads = int(os.environ.get("ORT_THREADS", "1"))
        self._session = ort.InferenceSession(
            str(onnx_path), sess_options=opts, providers=["CPUExecutionProvider"]
        )
        self._tokenizer = AutoTokenizer.from_pretrained(str(tokenizer_dir))
        self._max_len = max_len
        self._input_names = {i.name for i in self._session.get_inputs()}

    def score(self, text: str) -> Score:
        return self.score_batch([text])[0]

    def score_batch(self, texts: Iterable[str]) -> list[Score]:
        texts = [t or "" for t in texts]
        enc = self._tokenizer(
            texts, padding=True, truncation=True,
            max_length=self._max_len, return_tensors="np",
        )
        feeds = {k: v for k, v in enc.items() if k in self._input_names}
        logits = self._session.run(None, feeds)[0]
        probs = _softmax(logits)
        out: list[Score] = []
        for row in probs:
            idx = int(row.argmax())
            out.append(Score(
                label=_LABELS[idx],
                neg_prob=float(row[0]),
                confidence=float(row[idx]),
            ))
        return out


# ---------------------------------------------------------------------------
# Singleton factory
# ---------------------------------------------------------------------------

_SCORER = None


def get_scorer() -> _VaderScorer | _OnnxScorer:
    global _SCORER
    if _SCORER is None:
        backend = os.environ.get("SENTIMENT_BACKEND", "vader").lower()
        if backend == "onnx":
            _SCORER = _OnnxScorer()
        else:
            _SCORER = _VaderScorer()
    return _SCORER
