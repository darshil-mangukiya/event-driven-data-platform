# k6 Load Testing

Python load scripts are the default local runner. k6 scripts are included for a more production-like API stress-test shape.

## Ingestion Batch Load

```bash
k6 run benchmarks/k6/ingestion_batch_load.js
```

Useful overrides:

```bash
BASE_URL=http://localhost:8001 TENANT_ID=tenant_demo VUS=20 DURATION=3m BATCH_SIZE=50 \
  k6 run benchmarks/k6/ingestion_batch_load.js
```

## Analytics Read Load

```bash
k6 run benchmarks/k6/analytics_read_load.js
```

## Benchmark Comparison

Compare a local run result to the checked-in sample baseline:

```bash
python scripts/compare_benchmarks.py \
  --current benchmarks/results/local-run.json \
  --baseline samples/benchmarks/local_ingestion_sample.json \
  --pretty
```

## Production Boundary

Laptop k6 runs are smoke/performance probes. Production throughput claims require distributed load generation, broker sizing, database write-path profiling, and observability on queue lag and API saturation.
