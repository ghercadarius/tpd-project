"""Thin wrapper: export the trained HF model to ONNX + int8."""
from model.export_onnx import main

if __name__ == "__main__":
    main()
