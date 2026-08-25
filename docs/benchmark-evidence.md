# Benchmark Evidence

No benchmark JSON files are checked in as claimed results because the full Docker stack was not started in this environment.

Generate measured local evidence with:

```bash
python scripts/load_test_events.py --output benchmarks/results/local-run.json
python scripts/benchmark_report.py --output docs/benchmark-evidence.md
```

Production-scale targets should be validated separately with distributed load generation, Kafka broker metrics, Postgres write latency, Redis hit rate, and API saturation measurements.

