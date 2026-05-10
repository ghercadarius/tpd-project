"""Export the fine-tuned HF model to ONNX and produce an int8-quantized variant."""
from __future__ import annotations

import argparse
import logging
import shutil
from pathlib import Path

LOG = logging.getLogger("export")


def export(model_dir: Path, out_dir: Path) -> None:
    from optimum.onnxruntime import ORTModelForSequenceClassification
    from onnxruntime.quantization import quantize_dynamic, QuantType
    from transformers import AutoTokenizer

    out_dir.mkdir(parents=True, exist_ok=True)

    LOG.info("Exporting %s -> ONNX (fp32) ...", model_dir)
    ort_model = ORTModelForSequenceClassification.from_pretrained(model_dir, export=True)
    fp32_dir = out_dir / "onnx_fp32"
    ort_model.save_pretrained(fp32_dir)

    fp32_path = fp32_dir / "model.onnx"
    int8_path = out_dir / "sentiment.int8.onnx"
    LOG.info("Quantizing -> %s", int8_path)
    quantize_dynamic(
        model_input=str(fp32_path),
        model_output=str(int8_path),
        weight_type=QuantType.QInt8,
    )

    # Save a tokenizer bundle next to the int8 model.
    tok_dir = out_dir / "tokenizer"
    tok_dir.mkdir(exist_ok=True)
    AutoTokenizer.from_pretrained(model_dir).save_pretrained(tok_dir)

    # Friendly fp32 alias.
    shutil.copy(fp32_path, out_dir / "sentiment.onnx")
    LOG.info("Done. Artifacts in %s", out_dir)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--model-dir", type=Path, default=Path("model/artifacts/final"))
    p.add_argument("--out-dir", type=Path, default=Path("model/artifacts"))
    args = p.parse_args()
    export(args.model_dir, args.out_dir)


if __name__ == "__main__":
    main()
