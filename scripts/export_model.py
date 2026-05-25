"""Export a pretrained HF sentiment model to ONNX (fp32 + int8 quantised).

Only needed when SENTIMENT_BACKEND=onnx.  VADER works without this step.

Usage:
    python scripts/export_model.py [--model-id <hf-id>] [--out-dir model/artifacts]
"""
from model.export_onnx import main

if __name__ == "__main__":
    main()
