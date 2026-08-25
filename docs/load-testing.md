# Load Testing

Local load test:

```bash
python scripts/load_test_events.py --batches 20 --batch-size 50
```

Larger local run:

```bash
python scripts/load_test_events.py --batches 200 --batch-size 100
```

k6 API stress scripts:

```bash
k6 run benchmarks/k6/ingestion_batch_load.js
k6 run benchmarks/k6/analytics_read_load.js
```

See `docs/k6-load-testing.md` for environment overrides and benchmark comparison.

Measured output includes:

- Total events.
- Elapsed time.
- Events per second.
- Failed batches.
- Median batch latency.
- Max batch latency.

Production-scale statements in this project are architectural targets, not local benchmark claims. The local scripts are intentionally bounded so a laptop can run the full stack.
