"""Brand crisis detection PyFlink job.

Pipeline:
  1. Sources     — Kafka(reddit.raw.submissions) ∪ Kafka(reddit.raw.comments)
  2. Watermark   — event-time from `created_utc`, bounded out-of-orderness
  3. EnrichMap   — clean text, drop bots/blanks
  4. SentimentMap— VADER (or ONNX) score → reddit.scored
  5. KeyBy(brand)→ SlidingEventTimeWindow → BrandWindowAgg → reddit.aggregates
  6. SpikeDetector (KeyedProcessFunction) → reddit.alerts

Run locally:
    python scripts/download_jars.py   # once
    python -m flink_jobs.brand_crisis_job

Requirements: apache-flink==1.19.1, Python 3.8–3.11, Java 11+
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
import time
from pathlib import Path

import yaml
from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import FlatMapFunction, MapFunction, ProcessWindowFunction
from pyflink.datastream.window import SlidingEventTimeWindows

from flink_jobs.operators.spike_detector import SpikeDetector

LOG = logging.getLogger("brand_crisis_job")

_URL_RE = re.compile(r"https?://\S+")
_MD_RE = re.compile(r"[*_>~`]+")
_BOT_RE = re.compile(r"(bot|automoderator)$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def _load_detection_cfg() -> dict:
    for p in ("config/detection.yml", "/opt/flink/usrlib/config/detection.yml"):
        path = Path(p)
        if path.exists():
            return yaml.safe_load(path.read_text()).get("detection", {}).get("defaults", {})
    return {}


def _brand_key(value: str) -> str:
    """Best-effort key extractor that never raises inside Python workers."""
    try:
        rec = json.loads(value) if isinstance(value, (str, bytes)) else value
        brand = rec.get("brand", "_") if isinstance(rec, dict) else "_"
        return str(brand or "_")
    except Exception:
        return "_"


# ---------------------------------------------------------------------------
# Watermark
# ---------------------------------------------------------------------------

class _UtcAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_ts: int) -> int:
        try:
            return int(float(json.loads(value).get("created_utc", 0)) * 1000)
        except Exception:
            return record_ts


# ---------------------------------------------------------------------------
# Operators
# ---------------------------------------------------------------------------

class EnrichMap(MapFunction):
    """Clean text, drop bot authors and blank records. Returns None to filter."""

    def map(self, value: str):
        try:
            rec = json.loads(value)
        except Exception:
            return None
        author = (rec.get("author") or "").strip()
        if author and _BOT_RE.search(author):
            return None
        text = " ".join(filter(None, [rec.get("title"), rec.get("body")]))
        text = _URL_RE.sub(" ", text)
        text = _MD_RE.sub(" ", text)
        text = re.sub(r"\s+", " ", text).strip()
        if len(text) < 5:
            return None
        rec["text"] = text[:512]
        return json.dumps(rec)


class SentimentMap(MapFunction):
    """Score text; initialise the scorer lazily per TaskManager process."""

    _scorer = None

    def open(self, ctx):
        try:
            from model.inference import get_scorer
            self._scorer = get_scorer()
        except Exception as e:
            LOG.exception("sentiment scorer init failed; falling back to neutral scoring: %s", e)

            class _NeutralScorer:
                def score(self, _text: str):
                    class _S:
                        label = "neu"
                        neg_prob = 0.0
                    return _S()

            self._scorer = _NeutralScorer()

    def map(self, value: str):
        if not value:
            return None
        try:
            rec = json.loads(value)
        except Exception:
            return None
        s = self._scorer.score(rec.get("text", ""))
        rec["label"] = s.label
        rec["neg_prob"] = round(s.neg_prob, 4)
        return json.dumps(rec)


class BrandWindowAgg(ProcessWindowFunction):
    """Per-brand window aggregate: volume, neg counts, EWMA inputs."""

    def process(self, key: str, ctx, elements):
        volume = neg_count = 0
        neg_prob_sum = influencer_neg = 0.0
        authors: set[str] = set()
        sample_text: str | None = None
        worst_neg = 0.0

        for raw in elements:
            try:
                rec = json.loads(raw)
            except Exception:
                continue
            volume += 1
            neg_p = float(rec.get("neg_prob", 0.0))
            neg_prob_sum += neg_p
            if rec.get("label") == "neg":
                neg_count += 1
            if rec.get("author"):
                authors.add(rec["author"])
            weight = math.log1p(max(0, int(rec.get("score") or 0)))
            influencer_neg += neg_p * weight
            if neg_p > worst_neg:
                worst_neg = neg_p
                sample_text = (rec.get("text") or "")[:280]

        if volume == 0:
            return

        win = ctx.window()
        yield json.dumps({
            "brand": key,
            "window_start": win.start / 1000.0,
            "window_end": win.end / 1000.0,
            "volume": volume,
            "neg_count": neg_count,
            "neg_ratio": round(neg_count / volume, 4),
            "avg_neg_prob": round(neg_prob_sum / volume, 4),
            "unique_authors": len(authors),
            "influencer_neg": round(influencer_neg, 4),
            "sample_text": sample_text,
        })


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def _source(topic: str, bootstrap: str, group: str) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics(topic)
        .set_group_id(group)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def _sink(topic: str, bootstrap: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------

def _add_kafka_jar(env: StreamExecutionEnvironment) -> None:
    """Register the Kafka connector fat-jar so KafkaSource/KafkaSink can be resolved."""
    jar_dir = Path(__file__).parent / "jars"
    jars = list(jar_dir.glob("flink-sql-connector-kafka-*.jar"))
    if not jars:
        raise FileNotFoundError(
            f"Kafka connector JAR not found in {jar_dir}. "
            "Run:  python scripts/download_jars.py"
        )
    jar_uri = jars[0].resolve().as_uri()
    env.add_jars(jar_uri)
    LOG.info("loaded JAR: %s", jar_uri)


def build_job(env: StreamExecutionEnvironment) -> None:
    cfg = _load_detection_cfg()
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")

    ooo_s = int(cfg.get("out_of_orderness_seconds", 30))
    win_m = int(cfg.get("window_size_minutes", 5))
    slide_m = int(cfg.get("window_slide_minutes", 1))

    _add_kafka_jar(env)
    env.set_parallelism(int(os.environ.get("FLINK_PARALLELISM", "2")))
    env.enable_checkpointing(30_000, CheckpointingMode.EXACTLY_ONCE)

    wm = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(ooo_s))
        .with_timestamp_assigner(_UtcAssigner())
    )

    group = "flink-brand-crisis"
    src_sub = env.from_source(
        _source(os.environ.get("TOPIC_RAW_SUBMISSIONS", "reddit.raw.submissions"), bootstrap, group),
        wm, "kafka-submissions",
    )
    src_com = env.from_source(
        _source(os.environ.get("TOPIC_RAW_COMMENTS", "reddit.raw.comments"), bootstrap, group),
        wm, "kafka-comments",
    )

    enriched = (
        src_sub.union(src_com)
        .map(EnrichMap(), output_type=Types.STRING())
        .filter(lambda x: x is not None)
        .name("enrich")
    )

    scored = (
        enriched
        .map(SentimentMap(), output_type=Types.STRING())
        .filter(lambda x: x is not None)
        .name("sentiment")
    )
    scored.sink_to(
        _sink(os.environ.get("TOPIC_SCORED", "reddit.scored"), bootstrap)
    ).name("sink-scored")

    aggregates = (
        scored
        .key_by(_brand_key, key_type=Types.STRING())
        .window(SlidingEventTimeWindows.of(Time.minutes(win_m), Time.minutes(slide_m)))
        .process(BrandWindowAgg(), output_type=Types.STRING())
        .name("aggregate")
    )
    aggregates.sink_to(
        _sink(os.environ.get("TOPIC_AGGREGATES", "reddit.aggregates"), bootstrap)
    ).name("sink-aggregates")

    detector = SpikeDetector(
        alpha=float(cfg.get("ewma_alpha", 0.3)),
        spike_k=float(cfg.get("spike_k", 3.0)),
        min_volume=int(cfg.get("min_volume", 25)),
        neg_ratio_threshold=float(cfg.get("neg_ratio_threshold", 0.45)),
        cooldown_seconds=int(cfg.get("cooldown_minutes", 15)) * 60,
    )
    alerts = (
        aggregates
        .key_by(_brand_key, key_type=Types.STRING())
        .process(detector, output_type=Types.STRING())
        .name("spike-detector")
    )
    alerts.sink_to(
        _sink(os.environ.get("TOPIC_ALERTS", "reddit.alerts"), bootstrap)
    ).name("sink-alerts")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    env = StreamExecutionEnvironment.get_execution_environment()
    build_job(env)
    env.execute("brand-crisis")


if __name__ == "__main__":
    main()
