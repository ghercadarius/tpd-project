# Architecture

## Goal
A lightweight real-time pipeline that ingests Reddit content (historical replay
+ live), scores sentiment per message, detects negative spikes per monitored
brand, and exposes trends and alerts on a dashboard.

## High-level diagram

```mermaid
flowchart LR
    PS[Pushshift NDJSON/zst dump] --> P1[pushshift_replay.py]
    RA[Reddit official API] --> P2[reddit_live.py PRAW]
    P1 -- key=brand --> KS[(reddit.raw.submissions)]
    P1 -- key=brand --> KC[(reddit.raw.comments)]
    P2 -- key=brand --> KS
    P2 -- key=brand --> KC

    subgraph Flink["PyFlink job (DataStream API)"]
      direction TB
      EN[Enrich Map\n(cleanup/lang/bot)] --> SE[Sentiment FlatMap\n(ONNX UDF)]
      SE --> KW[KeyBy(brand)\nSliding 5m / 1m]
      KW --> AG[ProcessWindow\nper-brand aggregates]
      AG --> SD[KeyedProcessFunction\nEWMA SpikeDetector]
    end

    KS --> Flink
    KC --> Flink
    SE --> KSC[(reddit.scored)]
    AG --> KAG[(reddit.aggregates)]
    SD --> KAL[(reddit.alerts)]

    KAG --> SC[sink_consumer.py]
    KAL --> SC
    SC --> PG[(Postgres)]

    PG --> API[FastAPI]
    KAL --> API
    API --> UI[Streamlit dashboard]
```

## Components

| Component | Path | Responsibility |
|---|---|---|
| Pushshift replay producer | `producers/pushshift_replay.py` | Stream historical dumps to Kafka (rate-controlled). |
| Live Reddit producer | `producers/reddit_live.py` | PRAW-based stream of submissions + comments. |
| Sentiment model | `model/{train,export_onnx,inference}.py` | Pretrained checkpoint export; int8 ONNX; CPU-served scorer. |
| Flink job | `flink_jobs/brand_crisis_job.py` | Watermarked event-time pipeline: enrich → score → window → spike-detect. |
| Spike detector | `flink_jobs/operators/spike_detector.py` | EWMA z-score with cooldown, keyed by brand. |
| Sink consumer | `dashboard/sink_consumer.py` | Materialize aggregates + alerts into Postgres. |
| API | `dashboard/api.py` | REST + SSE endpoints. |
| UI | `dashboard/app.py` | Streamlit charts + alerts panel. |

## Kafka topics

| Topic | Partitions | Key | Retention | Producer | Consumer |
|---|---|---|---|---|---|
| `reddit.raw.submissions` | 12 | brand | 7d | producers | Flink |
| `reddit.raw.comments` | 12 | brand | 7d | producers | Flink |
| `reddit.scored` | 12 | brand | 3d | Flink | (introspection) |
| `reddit.aggregates` | 6 | brand | 30d | Flink | sink_consumer |
| `reddit.alerts` | 3 | brand | 30d | Flink | sink_consumer + API SSE |

Why key by brand: it preserves per-brand ordering, co-locates the spike-detector
state and the influencer cache after `keyBy`, and minimizes shuffle cost. Topic
partition counts are tuned so brand keys can spread across parallel Flink
subtasks while still keeping a single brand on a single partition (per Stream
Processing summary §4 and Windows summary §2).

## Time semantics
* **Event time** is taken from `created_utc`; this gives deterministic results
  during Pushshift replay (Windows summary §1).
* **Watermark** strategy: `forBoundedOutOfOrderness(30s)` — accommodates Reddit
  timestamp jitter and Kafka producer batching without unduly delaying windows.
* **Windows**: sliding event-time `size=5min, slide=1min`. Sliding (vs tumbling)
  smooths burst detection and lets the EWMA observe overlapping evidence.

## State & fault tolerance
* RocksDB state backend on `KeyedProcessFunction` (Stream Processing summary §5).
* Exactly-once checkpoints every 30s; Kafka source/sink configured for
  exactly-once via two-phase commit.
* The spike detector keeps two pieces of keyed state per brand:
  `ValueState<EWMA{mean, var, n}>` and `ValueState<lastAlertTs>` (cooldown).

## Lambda-style validation → production
The job is source-agnostic: the same operator graph runs against either
`pushshift_replay.py` (historical replay used to validate detection thresholds
against known crisis windows) or `reddit_live.py` (production). This matches
the Lambda-architecture pattern from the Data Processing Models summary §2.

## Out of scope (v1)
- Local sentiment training.
- Multi-language support (English only).
- Reddit write/moderation actions.
- Multi-region deployment.
