"""Shared helpers for Reddit producers."""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class Brand:
    name: str
    keywords: tuple[str, ...]
    subreddits: tuple[str, ...]


def load_brands(path: str | Path = "config/brands.yml") -> list[Brand]:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return [
        Brand(
            name=b["name"],
            keywords=tuple(k.lower() for k in b.get("keywords", [])),
            subreddits=tuple(b.get("subreddits", ["all"])),
        )
        for b in data.get("brands", [])
    ]


def match_brand(text: str, brands: list[Brand]) -> str | None:
    """Return the first brand whose keyword appears in the text (case-insensitive)."""
    if not text:
        return None
    t = text.lower()
    for brand in brands:
        for kw in brand.keywords:
            if re.search(rf"\b{re.escape(kw)}\b", t):
                return brand.name
    return None
