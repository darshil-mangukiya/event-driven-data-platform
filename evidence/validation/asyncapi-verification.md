# AsyncAPI Event Architecture Verification

Status: **VERIFIED**

Date: 2026-08-21

`contracts/asyncapi.yml` is generated from Kafka topic definitions, the
contract registry, and publisher/subscriber mappings.

| Check | Result |
| --- | --- |
| Generated document | 7 channels, 18 operations, 7 messages |
| YAML parsing | PASS |
| Topic/channel cross-reference | PASS |
| Schema-file references | PASS |
| Event-type references | PASS |
| Unknown-channel regression test | PASS |

Covered topics are the five domain topics plus retry and DLQ. Retry and DLQ
use the shared event envelope with additional metadata.

```bash
python scripts/generate_asyncapi.py
python scripts/validate_asyncapi.py
pytest -q tests/test_asyncapi.py
```

The repository does not include the Node-based AsyncAPI parser. Validation
covers YAML structure and project-specific cross-references. W3C trace-context
propagation is validated separately by the tracing tests.
