"""Unit test for SentimentScorer.

Skipped automatically if the ONNX artifact is not present, so the test suite
is green even before the model has been trained.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

ONNX = Path(os.environ.get("ONNX_MODEL", "model/artifacts/sentiment.int8.onnx"))
TOK = Path(os.environ.get("TOKENIZER_DIR", "model/artifacts/tokenizer"))


@pytest.mark.skipif(not (ONNX.exists() and TOK.exists()),
                    reason="ONNX artifact not built yet")
def test_score_basic():
    from model.inference import SentimentScorer

    s = SentimentScorer()
    pos = s.score("I love this product, absolutely fantastic!")
    neg = s.score("Worst purchase ever, totally broken and useless.")
    assert 0.0 <= pos.neg_prob <= 1.0
    assert 0.0 <= neg.neg_prob <= 1.0
    # Sanity: negative text should have higher neg_prob than positive text.
    assert neg.neg_prob > pos.neg_prob
