"""Brand crisis detection PyFlink job.

Pipeline (DataStream API):
    1. Sources         : Kafka(reddit.raw.submissions) + Kafka(reddit.raw.comments)
    2. Watermark       : event-time from `created_utc`, 30s out-of-orderness
    3. Enrichment Map  : cleanup, lang filter, bot drop
    4. Sentiment UDF   : ONNX scorer (lazy, per TaskManager) -> reddit.scored
    5. KeyBy(brand)    : sliding 5min/1min event-time window
    6. ProcessWindow   : per-brand aggregates -> reddit.aggregates
    7. SpikeDetector   : KeyedProcessFunction with EWMA state -> reddit.alerts

Run:
    python flink_jobs/brand_crisis_job.py            # local mini-cluster
    flink run -py flink_jobs/brand_crisis_job.py     # against a cluster
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
    KafkaOffsetsInitializer, KafkaSink, KafkaSource, KafkaRecordSerializationSchema,
)
from pyflink.datastream.functions import (
    FlatMapFunction, MapFunction, ProcessWindowFunction,
)
from pyflink.datastream.window import SlidingEventTimeWindows

from flink_jobs.operators.spike_detector import SpikeDetector

LOG = logging.getLogger("brand_crisis_job")

URL_RE = re.compile(r"https?://\S+")
MD_RE = re.compile(r"[*_>~`]+")
BOT_RE = re.compile(r"(bot|automoderator)$", re.IGNORECASE)


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #
def load_detection_config(path: str = "config/detection.yml") -> dict:
    p = Path(path)
    if not p.exists():
        # When running inside the Flink container the config is mounted under usrlib.
        alt = Path("/opt/flink/usrlib/config/detection.yml")
        if alt.exists():
            p = alt
    return yaml.safe_load(p.read_text(encoding="utf-8")).get("detection", {}).get("defaults", {})


class _EventTimeAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        try:
            rec = json.loads(value)
            return int(float(rec.get("created_utc", time.time())) * 1000)
        except Exception:
            return record_timestamp


# --------------------------------------------------------------------------- #
# Operators                                                                   #
# --------------------------------------------------------------------------- #
class EnrichMap(MapFunction):
    """Cleanup + language/bot filtering. Returns enriched JSON or None to drop."""

    def map(self, value):
        try:
            rec = json.loads(value)
        except Exception:
            return None
        author = (rec.get("author") or "").strip()
        if author and BOT_RE.search(author):
            return None
        text = " ".join(filter(None, [rec.get("title"), rec.get("body")]))
        text = URL_RE.sub(" ", text)
        text = MD_RE.sub(" ", text)
        text = re.sub(r"\s+", " ", text).strip()
        if len(text) < 3:
            return None
        rec["text"] = text[:512]
        return json.dumps(rec)


class SentimentFlatMap(FlatMapFunction):
    """Lazy ONNX scorer; scores text and emits an enriched 'scored' record."""

    def open(self, ctx):
        # Import lazily so the job can be parsed even when the artifact is absent.
        from model.inference import get_scorer
        self._scorer = get_scorer()

    def flat_map(self, value):
        if value is None:
            return
        try:
            rec = json.loads(value)
        except Exception:
            return
        s = self._scorer.score(rec.get("text", ""))
        out = {
            "id": rec.get("id"),
            "type": rec.get("type"),
            "subreddit": rec.get("subreddit"),
            "author": rec.get("author"),
            "brand": rec.get("brand"),
            "text": rec.get("text"),
            "permalink": rec.get("permalink"),
            "created_utc": rec.get("created_utc"),
            "label": s.label,
            "neg_prob": s.neg_prob,
            "score": rec.get("score", 0),
        }
        yield json.dumps(out)


class BrandWindowAgg(ProcessWindowFunction):
    """Compute per-brand aggregates for a sliding window of scored records."""

    def process(self, key, context, elements):
        volume = 0
        neg_count = 0
        neg_prob_sum = 0.0
        authors: set[str] = set()
        influencer_neg = 0.0
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
            # Influencer weight: log(1 + score) approximates audience reach.
            weight = math.log1p(max(0, int(rec.get("score") or 0)))
            influencer_neg += neg_p * weight
            if neg_p > worst_neg:
                worst_neg = neg_p
                sample_text = (rec.get("text") or "")[:280]

        win = context.window()
        agg = {
            "brand": key,
            "window_start": win.start / 1000.0,
            "window_end": win.end / 1000.0,
            "volume": volume,
            "neg_count": neg_count,
            "neg_ratio": (neg_count / volume) if volume else 0.0,
            "avg_neg_prob": (neg_prob_sum / volume) if volume else 0.0,
            "unique_authors": len(authors),
            "influencer_neg": influencer_neg,
            "sample_text": sample_text,
        }
        yield json.dumps(agg)


# --------------------------------------------------------------------------- #
# Job builder                                                                 #
# --------------------------------------------------------------------------- #
def _kafka_source(topic: str, bootstrap: str, group: str) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics(topic)
        .set_group_id(group)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def _kafka_sink(topic: str, bootstrap: str) -> KafkaSink:
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


def build_job(env: StreamExecutionEnvironment) -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    cfg = load_detection_config()
    out_of_orderness = int(cfg.get("out_of_orderness_seconds", 30))
    win_size = int(cfg.get("window_size_minutes", 5))
    win_slide = int(cfg.get("window_slide_minutes", 1))

    env.set_parallelism(int(os.environ.get("FLINK_PARALLELISM", "4")))
    env.enable_checkpointing(30_000, CheckpointingMode.EXACTLY_ONCE)

    wm = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(out_of_orderness))
        .with_timestamp_assigner(_EventTimeAssigner())
    )

    src_sub = env.from_source(
        _kafka_source(os.environ.get("TOPIC_RAW_SUBMISSIONS", "reddit.raw.submissions"),
                      bootstrap, "flink-brand-crisis"),
        wm, "kafka-submissions"
    )
    src_com = env.from_source(
        _kafka_source(os.environ.get("TOPIC_RAW_COMMENTS", "reddit.raw.comments"),
                      bootstrap, "flink-brand-crisis"),
        wm, "kafka-comments"
    )
    raw = src_sub.union(src_com)

    enriched = (
        raw.map(EnrichMap(), output_type=Types.STRING())
           .filter(lambda x: x is not None)
           .name("enrich")
    )

    scored = enriched.flat_map(SentimentFlatMap(), output_type=Types.STRING()).name("sentiment")
    scored.sink_to(_kafka_sink(os.environ.get("TOPIC_SCORED", "reddit.scored"), bootstrap)) \
          .name("sink-scored")

    aggregates = (
        scored.key_by(lambda v: json.loads(v).get("brand", "_"), key_type=Types.STRING())
              .window(SlidingEventTimeWindows.of(
                  Time.minutes(win_size), Time.minutes(win_slide)))
              .process(BrandWindowAgg(), output_type=Types.STRING())
              .name("aggregate")
    )
    aggregates.sink_to(_kafka_sink(os.environ.get("TOPIC_AGGREGATES", "reddit.aggregates"),
                                   bootstrap)).name("sink-aggregates")

    detector = SpikeDetector(
        alpha=float(cfg.get("ewma_alpha", 0.3)),
        spike_k=float(cfg.get("spike_k", 3.0)),
        min_volume=int(cfg.get("min_volume", 25)),
        neg_ratio_threshold=float(cfg.get("neg_ratio_threshold", 0.45)),
        cooldown_seconds=int(cfg.get("cooldown_minutes", 15)) * 60,
    )
    alerts = (
        aggregates.key_by(lambda v: json.loads(v).get("brand", "_"), key_type=Types.STRING())
                  .process(detector, output_type=Types.STRING())
                  .name("spike-detector")
    )
    alerts.sink_to(_kafka_sink(os.environ.get("TOPIC_ALERTS", "reddit.alerts"), bootstrap)) \
          .name("sink-alerts")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    env = StreamExecutionEnvironment.get_execution_environment()
    build_job(env)
    env.execute("brand-crisis")


if __name__ == "__main__":
    main()
