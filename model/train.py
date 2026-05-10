"""Fine-tune a small transformer for 3-class Reddit sentiment on a local GPU."""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from sklearn.metrics import accuracy_score, f1_score
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    DataCollatorWithPadding,
    Trainer,
    TrainingArguments,
)
from datasets import Dataset

LOG = logging.getLogger("train")
LABELS = ["neg", "neu", "pos"]
LABEL2ID = {l: i for i, l in enumerate(LABELS)}
ID2LABEL = {i: l for l, i in LABEL2ID.items()}


def _load(parquet: Path) -> Dataset:
    df = pd.read_parquet(parquet)
    df["labels"] = df["label"].map(LABEL2ID)
    return Dataset.from_pandas(df[["text", "labels"]], preserve_index=False)


def _metrics(pred):
    logits, labels = pred
    preds = np.argmax(logits, axis=-1)
    return {
        "accuracy": accuracy_score(labels, preds),
        "f1_macro": f1_score(labels, preds, average="macro"),
    }


def train(args: argparse.Namespace) -> None:
    device = "cuda" if torch.cuda.is_available() and not args.cpu else "cpu"
    LOG.info("Training on device=%s, model=%s", device, args.model)

    tokenizer = AutoTokenizer.from_pretrained(args.model)
    model = AutoModelForSequenceClassification.from_pretrained(
        args.model, num_labels=len(LABELS), id2label=ID2LABEL, label2id=LABEL2ID
    )

    train_ds = _load(args.data_dir / "train.parquet")
    val_ds = _load(args.data_dir / "val.parquet")
    test_ds = _load(args.data_dir / "test.parquet")

    def tok(batch):
        return tokenizer(batch["text"], truncation=True, max_length=args.max_len)

    train_ds = train_ds.map(tok, batched=True, remove_columns=["text"])
    val_ds = val_ds.map(tok, batched=True, remove_columns=["text"])
    test_ds = test_ds.map(tok, batched=True, remove_columns=["text"])

    collator = DataCollatorWithPadding(tokenizer=tokenizer)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    training_args = TrainingArguments(
        output_dir=str(args.out_dir / "checkpoints"),
        num_train_epochs=args.epochs,
        per_device_train_batch_size=args.batch_size,
        per_device_eval_batch_size=args.batch_size * 2,
        learning_rate=args.lr,
        weight_decay=0.01,
        warmup_ratio=0.06,
        eval_strategy="epoch",
        save_strategy="epoch",
        load_best_model_at_end=True,
        metric_for_best_model="f1_macro",
        fp16=(device == "cuda"),
        logging_steps=50,
        save_total_limit=2,
        report_to=[],
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_ds,
        eval_dataset=val_ds,
        tokenizer=tokenizer,
        data_collator=collator,
        compute_metrics=_metrics,
    )
    trainer.train()

    test_metrics = trainer.evaluate(test_ds)
    LOG.info("Test metrics: %s", test_metrics)

    final_dir = args.out_dir / "final"
    trainer.save_model(str(final_dir))
    tokenizer.save_pretrained(str(final_dir))

    run = {
        "base_model": args.model,
        "epochs": args.epochs,
        "batch_size": args.batch_size,
        "lr": args.lr,
        "max_len": args.max_len,
        "device": device,
        "test_metrics": {k: float(v) for k, v in test_metrics.items() if isinstance(v, (int, float))},
        "labels": LABELS,
    }
    (args.out_dir / "run.json").write_text(json.dumps(run, indent=2))
    LOG.info("Saved model to %s", final_dir)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--model", default="distilbert-base-uncased")
    p.add_argument("--data-dir", type=Path, default=Path("data/train"))
    p.add_argument("--out-dir", type=Path, default=Path("model/artifacts"))
    p.add_argument("--epochs", type=int, default=3)
    p.add_argument("--batch-size", type=int, default=32)
    p.add_argument("--lr", type=float, default=5e-5)
    p.add_argument("--max-len", type=int, default=128)
    p.add_argument("--cpu", action="store_true", help="Force CPU training.")
    train(p.parse_args())


if __name__ == "__main__":
    main()
