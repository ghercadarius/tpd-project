# Flink job & detection logic

## Operator graph

```
KafkaSource(reddit.raw.submissions) ─┐
                                     ├──► union ──► EnrichMap ──► SentimentFlatMap ──► KafkaSink(reddit.scored)
KafkaSource(reddit.raw.comments)   ──┘                                  │
                                                                        ▼
                                                       keyBy(brand) ──► SlidingEventTimeWindows(5m, 1m)
                                                                        ▼
                                                              BrandWindowAgg ──► KafkaSink(reddit.aggregates)
                                                                        │
                                                                        ▼
                                                              keyBy(brand) ──► SpikeDetector
                                                                        │
                                                                        ▼
                                                                 KafkaSink(reddit.alerts)
```

## Per-operator state & semantics

| Operator | Type | State | Notes |
|---|---|---|---|
| `EnrichMap` | `MapFunction` | none | URL/markdown strip, language stub, bot drop. |
| `SentimentFlatMap` | `FlatMapFunction` | per-process ONNX session | `open()` lazy-loads `SentimentScorer`. |
| `BrandWindowAgg` | `ProcessWindowFunction` | none beyond window contents | Computes `volume`, `neg_count`, `neg_ratio`, `avg_neg_prob`, `unique_authors`, `influencer_neg = Σ neg_prob · log1p(score)`, `sample_text`. |
| `SpikeDetector` | `KeyedProcessFunction` | `ValueState<EWMA{mean,var,n}>`, `ValueState<lastAlertTs>` | Per-brand alerting with cooldown. |

Checkpointing is exactly-once @ 30s, RocksDB backend (Streaming summary §5).

## Detection math

For each per-window aggregate $a_t$ for brand $b$, the detector maintains an
exponentially-weighted mean and variance over `neg_count`:

$$\mu_t = (1-\alpha)\,\mu_{t-1} + \alpha\, x_t$$
$$\sigma^2_t = (1-\alpha)\big(\sigma^2_{t-1} + \alpha\,(x_t-\mu_{t-1})^2\big)$$

The z-score uses the *prior* statistics so the current point can spike:

$$z_t = \frac{x_t - \mu_{t-1}}{\sigma_{t-1}}\quad\text{(if }n\ge 5\text{)}$$

An alert fires iff:
- $z_t \geq k$ (default $k=3$),
- $\text{neg\_ratio}_t \geq r$ (default $0.45$), and
- $\text{volume}_t \geq v_{\min}$ (default $25$),
- and we are past `cooldown_minutes` since the last alert for the same brand.

Severity is bucketed from $z_t$: `critical ≥ 6`, `high ≥ 4.5`, `medium ≥ 3`,
otherwise `low`.

## Tuning knobs
All thresholds live in [config/detection.yml](../config/detection.yml). Use
`per_brand:` overrides for very high-volume brands.

| Knob | Default | Effect |
|---|---|---|
| `window_size_minutes` | 5 | Larger → smoother, slower alerting. |
| `window_slide_minutes` | 1 | Smaller → faster reaction, more compute. |
| `out_of_orderness_seconds` | 30 | Larger → more late events accepted. |
| `ewma_alpha` | 0.3 | Larger → adapts faster to new normal (less sensitive). |
| `spike_k` | 3.0 | Larger → fewer false positives, slower detection. |
| `min_volume` | 25 | Suppresses tiny-sample spikes. |
| `neg_ratio_threshold` | 0.45 | Filters bursts that aren't predominantly negative. |
| `cooldown_minutes` | 15 | Avoids alert storms during sustained crises. |

## Running the job

```bash
# Local (mini-cluster, for development)
python3 flink_jobs/brand_crisis_job.py

# Against the docker-compose cluster
bash scripts/submit_flink_job.sh
# or:
docker compose -f infra/docker-compose.yml exec jobmanager `
  flink run -d -p 4 --pyModule flink_jobs.brand_crisis_job `
  --pyFiles /opt/flink/usrlib/flink_jobs,/opt/flink/usrlib/model_artifacts,/opt/flink/usrlib/config
```

Inspect the live job at <http://localhost:8081>.
