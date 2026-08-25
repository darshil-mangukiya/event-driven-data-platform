# DLQ Replay Runbook

The DLQ tool supports inspect, dry-run replay, and audited replay.

Inspect recent DLQ records:

```bash
PYTHONPATH=services/shared python scripts/dlq_tool.py --max-records 10 inspect
```

Dry-run replay a specific event:

```bash
PYTHONPATH=services/shared python scripts/dlq_tool.py \
  --event-id <event_id> \
  replay \
  --dry-run \
  --database-url "$DATABASE_URL" \
  --reason "validated schema fix"
```

Replay after remediation:

```bash
PYTHONPATH=services/shared python scripts/dlq_tool.py \
  --event-id <event_id> \
  replay \
  --database-url "$DATABASE_URL" \
  --reason "processor deployed with fix"
```

Every replay attempt can be written to `dlq_replay_audit` with original event ID, tenant, source topic, target topic, status, operator, and reason.

Operational rule: never replay unknown poison events in bulk. Replay should follow root-cause fix, idempotency review, and audit logging.

