# Operations runbook

## Start everything from scratch (replay demo)

```bash
bash scripts/setup.sh
bash scripts/start_infra.sh
python3 scripts/export_model.py
python3 scripts/prepare_dataset.py
python3 scripts/eval_model.py
bash scripts/start_producers.sh --mode replay --file data/pushshift/sample.ndjson
bash scripts/submit_flink_job.sh
bash scripts/start_dashboard.sh
```

## Replay a known historical crisis window

1. Place the relevant Pushshift dump at `data/pushshift/<event>.ndjson(.zst)`.
2. Make sure `config/brands.yml` lists the affected brand and matching keywords.
3. Run:
   ```bash
   bash scripts/start_producers.sh --mode replay --file data/pushshift/<event>.ndjson --rate max
   ```
4. Watch the dashboard or query Postgres:
   ```sql
   SELECT * FROM alerts WHERE brand = '<brand>' ORDER BY triggered_at;
   ```

## Tune thresholds

1. Replay the dump at `--rate max`.
2. Inspect `aggregates_5m` for the brand around the known event time.
3. Edit `config/detection.yml` (e.g., lower `spike_k` if missed, raise
   `min_volume` if false positives).
4. Cancel and resubmit the Flink job (`Cancel` in the Flink UI, then
   `bash scripts/submit_flink_job.sh`).

## Smoke test the alerting path

```bash
python3 scripts/smoke_test.py --brand acme --count 200 --timeout 180
```

Expects an alert on `reddit.alerts` for `acme` within 3 minutes. Exit code
0 = pass, 1 = no alert, 2 = bad input.

## Rotate Reddit credentials
1. Generate a new client secret in <https://www.reddit.com/prefs/apps>.
2. Update `.env`.
3. Restart the live producer: `pkill -f reddit_live` then
   `bash scripts/start_producers.sh --mode live`.

## Recover from a failed checkpoint
1. Cancel the failed job in the Flink UI.
2. The latest completed checkpoint path is in the Flink UI under "Checkpoints".
3. Resubmit with that path:
   ```bash
   docker compose -f infra/docker-compose.yml exec jobmanager \
     flink run -s file:///opt/flink/checkpoints/<id>/chk-<n> \
     --pyModule flink_jobs.brand_crisis_job \
     --pyFiles /opt/flink/usrlib/flink_jobs,/opt/flink/usrlib/model_artifacts,/opt/flink/usrlib/config
   ```

## Common pitfalls

| Symptom | Likely cause | Fix |
|---|---|---|
| `ONNX model not found` in Flink logs | `model/artifacts/` not mounted into TaskManager. | Re-run `export_model.py`; ensure the volume mount in `docker-compose.yml` resolves. |
| No watermarks advancing | Producer not setting `created_utc`. | Check producer logs; raw events without `created_utc` are dropped by the watermark. |
| Brand never matched | Keywords too narrow. | Add aliases under `config/brands.yml`; restart producer. |
| Postgres "duplicate key" on aggregates | Multiple sink consumers running. | Stop extras; the upsert is idempotent but only one writer should exist. |
| Alert flood for one brand | `cooldown_minutes` too low. | Bump in `config/detection.yml` and resubmit job. |
