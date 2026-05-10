# Data pipeline & schemas

## End-to-end record lifecycle

```
raw (json, key=brand)  ──►  enriched (text cleaned)  ──►  scored
                                                          │
                                                          ├──► reddit.scored (Kafka)
                                                          ▼
                                            keyBy(brand) + sliding 5m/1m window
                                                          │
                                                          ▼
                                             aggregate {volume, neg_count, …}
                                                          ├──► reddit.aggregates
                                                          ▼
                                             EWMA spike detector (keyed state)
                                                          ├──► reddit.alerts
                                                          ▼
                                             sink_consumer → Postgres
```

## Schemas
JSON-Schema files live under `config/schemas/`. Summary:

### `reddit.raw.{submissions,comments}` — see [reddit_raw.json](../config/schemas/reddit_raw.json)
```json
{
  "id": "abc123",
  "type": "comment",
  "subreddit": "AskReddit",
  "author": "alice",
  "brand": "acme",
  "title": null,
  "body": "Acme support is terrible...",
  "permalink": "/r/.../",
  "score": 4,
  "parent_id": "t1_xyz",
  "created_utc": 1718000000.0,
  "ingested_at": 1718000003.1
}
```

### `reddit.scored` — see [reddit_scored.json](../config/schemas/reddit_scored.json)
Adds `text` (cleaned), `label ∈ {neg,neu,pos}`, `neg_prob ∈ [0,1]`.

### `reddit.aggregates`
Per-window per-brand record:
`{brand, window_start, window_end, volume, neg_count, neg_ratio, avg_neg_prob,
unique_authors, influencer_neg, sample_text}`.

### `reddit.alerts` — see [brand_alert.json](../config/schemas/brand_alert.json)
`{brand, triggered_at, window_start, window_end, z_score, neg_ratio, volume,
severity, sample_text}`.

## Source semantics

| Aspect | Pushshift replay | Reddit official API |
|---|---|---|
| Boundedness | Bounded (single dump) | Unbounded |
| Ordering | Mostly time-ordered, gaps from filtering | Approximate, with jitter |
| Rate control | `--rate realtime\|max\|<float>` | Push-based via PRAW streams |
| Auth | None (public dumps) | OAuth2 (env: `REDDIT_*`) |
| Use case | Validate detection thresholds | Production monitoring |

The Flink job sees an identical message schema regardless of source — that's
why the same code path is used for both validation and production.

## Partitioning rationale

We partition by **brand**, not subreddit, because:
1. The stateful operators (spike detector, influencer cache) are keyed by brand.
   Co-locating partition + key minimizes cross-task shuffle (Streaming summary §4).
2. A single subreddit (e.g., r/all) can mention many brands; partitioning by
   subreddit would still require a Flink shuffle to collect a brand's events.
3. With 12 partitions and ~4 brands today there is plenty of headroom; new
   brands hash to existing partitions transparently.

Trade-off: very popular brands can hot-spot a partition. If/when this happens,
upgrade to `brand#bucket` keying with N buckets and a stateless union before
the spike detector.
