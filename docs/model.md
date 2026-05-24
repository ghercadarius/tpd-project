# Sentiment model

A small pretrained transformer served as int8 ONNX inside the Flink TaskManager.
The default lifecycle no longer fine-tunes locally; it exports a published
checkpoint and optionally keeps the dataset/eval tools for experimentation.

## Pipeline

```
export_model  →  eval_model
   (ONNX + int8)    (gate)
```

| Step | Script | Output |
|---|---|---|
| Export pretrained checkpoint | `scripts/export_model.py` | `sentiment.onnx`, `sentiment.int8.onnx`, `tokenizer/` |
| Evaluate | `scripts/eval_model.py` | `baseline.json` (optional deploy gate) |

## Default checkpoint
The default export target is `cardiffnlp/twitter-roberta-base-sentiment-latest`.
It already provides a 3-class sentiment head, and the runtime normalizes labels
to the repo contract: `neg`, `neu`, `pos`.

## Optional dataset construction
`model/dataset.py` remains available for experiments and comparisons. It combines:
1. **Public corpus** — `tweet_eval/sentiment` from HuggingFace (3-class labels:
   neg/neu/pos). Provides high-quality supervision.
2. **Reddit weak labels (optional)** — pass `--reddit-dump path.ndjson[.zst]`
   to mix in Pushshift text auto-labeled by VADER (compound thresholds: ≤−0.3
   neg, ≥0.3 pos, else neu). Bridges the domain gap into Reddit-style writing.

Splits: 80 / 10 / 10. Deduplicated by exact text.

## Training compatibility shim
`model/train.py` and `scripts/train_model.py` no longer fine-tune locally.
They now act as compatibility wrappers that materialize the same pretrained
artifacts used by `scripts/export_model.py`.

## ONNX export & quantization
`model/export_onnx.py` uses `optimum.onnxruntime` to export the pretrained HF
checkpoint to ONNX (fp32) and `onnxruntime.quantization.quantize_dynamic` for
an int8 weight-quantized variant. Both files plus a tokenizer bundle live under
`model/artifacts/`.

Why int8: lets the model run efficiently on CPU inside Flink TaskManagers
(no GPU dependency in the streaming hot path), preserving the "lightweight"
goal from the task brief.

## Inference contract
`model/inference.SentimentScorer` is the single entry point used by both the
Flink UDF (`SentimentFlatMap.open`) and unit tests:

```python
from model.inference import SentimentScorer
s = SentimentScorer()                      # paths from env: ONNX_MODEL, TOKENIZER_DIR
out = s.score("Acme is awful")             # -> Score(label='neg', neg_prob=0.94, confidence=0.94)
```

The scorer is process-singleton (`get_scorer()`); each Flink TaskManager loads
the model exactly once.

## Evaluation gate
`scripts/eval_model.py` computes accuracy + macro-F1 on `data/train/test.parquet`
and compares against `model/artifacts/baseline.json`:
* On first run, writes the baseline.
* On regression beyond `--tolerance` (default 1% F1), exits non-zero — used as
  a CI / pre-deploy gate.
* On improvement, updates the baseline.
