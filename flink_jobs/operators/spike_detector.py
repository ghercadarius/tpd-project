"""Stateful spike detector for per-brand negative-sentiment aggregates.

Input:  per-window aggregate dicts keyed by brand.
State:  EWMA of `neg_count` (mean+variance) + last-alert timestamp (cooldown).
Output: alert dicts when current `neg_count` exceeds mean + k * stdev AND
        guardrails (min_volume, neg_ratio_threshold) are met AND we're past cooldown.
"""
from __future__ import annotations

import json
import logging
import math
import time

from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

LOG = logging.getLogger("spike_detector")


def _severity(z: float) -> str:
    if z >= 6.0:
        return "critical"
    if z >= 4.5:
        return "high"
    if z >= 3.0:
        return "medium"
    return "low"


class SpikeDetector(KeyedProcessFunction):
    """Detect negative-sentiment spikes per brand using an EWMA z-score."""

    def __init__(self,
                 alpha: float = 0.3,
                 spike_k: float = 3.0,
                 min_volume: int = 25,
                 neg_ratio_threshold: float = 0.45,
                 cooldown_seconds: int = 900) -> None:
        self.alpha = alpha
        self.spike_k = spike_k
        self.min_volume = min_volume
        self.neg_ratio_threshold = neg_ratio_threshold
        self.cooldown_seconds = cooldown_seconds
        self._stats = None
        self._last_alert = None

    def open(self, ctx):
        self._stats = ctx.get_state(
            ValueStateDescriptor("ewma_stats", Types.PICKLED_BYTE_ARRAY())
        )
        self._last_alert = ctx.get_state(
            ValueStateDescriptor("last_alert_ts", Types.LONG())
        )

    def process_element(self, value, ctx):
        agg = json.loads(value) if isinstance(value, (str, bytes)) else value
        brand = agg["brand"]
        neg_count = float(agg.get("neg_count", 0))
        volume = int(agg.get("volume", 0))
        neg_ratio = float(agg.get("neg_ratio", 0.0))

        stats = self._stats.value() or {"mean": 0.0, "var": 0.0, "n": 0}
        n = stats["n"]
        mean, var = stats["mean"], stats["var"]

        # Decision uses the *prior* statistics so the current point can spike.
        std = math.sqrt(var) if var > 0 else 0.0
        z = (neg_count - mean) / std if (std > 1e-6 and n >= 5) else 0.0

        # Update EWMA after computing z.
        if n == 0:
            new_mean, new_var = neg_count, 0.0
        else:
            diff = neg_count - mean
            new_mean = mean + self.alpha * diff
            new_var = (1 - self.alpha) * (var + self.alpha * diff * diff)
        self._stats.update({"mean": new_mean, "var": new_var, "n": n + 1})

        if (volume >= self.min_volume
                and neg_ratio >= self.neg_ratio_threshold
                and z >= self.spike_k):
            now_ms = ctx.timer_service().current_processing_time()
            last = self._last_alert.value() or 0
            if now_ms - last < self.cooldown_seconds * 1000:
                return
            self._last_alert.update(now_ms)
            alert = {
                "brand": brand,
                "triggered_at": now_ms / 1000.0,
                "window_start": agg["window_start"],
                "window_end": agg["window_end"],
                "z_score": float(z),
                "neg_ratio": neg_ratio,
                "volume": volume,
                "severity": _severity(z),
                "sample_text": agg.get("sample_text"),
            }
            LOG.info("ALERT %s z=%.2f vol=%d neg_ratio=%.2f", brand, z, volume, neg_ratio)
            yield json.dumps(alert)
