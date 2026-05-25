"""Export a pretrained HF sentiment model to ONNX (fp32 + int8 quantised).

Only needed when SENTIMENT_BACKEND=onnx.  VADER works without this step.

Usage:
    python scripts/export_model.py [--model-id <hf-id>] [--out-dir model/artifacts]
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from model.export_onnx import main

if __name__ == "__main__":
    main()
