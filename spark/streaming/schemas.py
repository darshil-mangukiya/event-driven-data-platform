"""Spark schemas mirroring contracts/schemas/v1 and contracts/events/*.

These are intentionally kept in lockstep with the JSON Schema contracts used
by the ingestion service (``contracts/schemas/v1/event-envelope.schema.json``
and the per-domain payload schemas). If the contracts change, update this
module alongside them — ``tests/streaming/test_schemas_match_contracts.py``
asserts the required envelope fields here match the JSON Schema's
``required`` list so the two cannot silently drift apart.
"""

from __future__ import annotations

from pyspark.sql import types as T

# Kafka's raw record shape (readStream.format("kafka") output columns we use).
KAFKA_RAW_COLUMNS = ("key", "value", "topic", "partition", "offset", "timestamp")

# The event envelope, as published to every domain topic. Field order and
# names match contracts/schemas/v1/event-envelope.schema.json.
EVENT_ENVELOPE_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), nullable=False),
        T.StructField("tenant_id", T.StringType(), nullable=False),
        T.StructField("event_type", T.StringType(), nullable=False),
        T.StructField("event_timestamp", T.StringType(), nullable=False),  # parsed later
        T.StructField("source_service", T.StringType(), nullable=False),
        T.StructField("payload_version", T.IntegerType(), nullable=True),
        T.StructField("payload", T.MapType(T.StringType(), T.StringType()), nullable=True),
        T.StructField("trace_id", T.StringType(), nullable=True),
        T.StructField("correlation_id", T.StringType(), nullable=True),
        T.StructField("causation_id", T.StringType(), nullable=True),
        T.StructField("idempotency_key", T.StringType(), nullable=True),
        T.StructField("schema_ref", T.StringType(), nullable=True),
    ]
)

# Envelope fields that MUST be present and non-null for an event to be
# considered structurally valid. Matches event-envelope.schema.json's
# "required" list minus payload (payload shape is validated per-domain).
REQUIRED_ENVELOPE_FIELDS = (
    "event_id",
    "tenant_id",
    "event_type",
    "event_timestamp",
    "source_service",
    "payload_version",
    "trace_id",
    "correlation_id",
    "idempotency_key",
)

# event_type -> domain, matches services/shared/platform_shared/schemas.py::EVENT_DOMAIN
EVENT_TYPE_DOMAIN: dict[str, str] = {
    "order.created": "orders",
    "order.updated": "orders",
    "payment.authorized": "payments",
    "payment.captured": "payments",
    "payment.failed": "payments",
    "user.signed_up": "users",
    "user.activity": "users",
    "user.churn_signal": "users",
    "product.upserted": "products",
    "product.inventory_changed": "products",
    "system.health": "system",
    "system.alert": "system",
}

KNOWN_EVENT_TYPES = tuple(EVENT_TYPE_DOMAIN.keys())

# Required payload fields per domain (mirrors contracts/schemas/v1/*-payload
# and services/shared/platform_shared/schemas.py Pydantic models). These are
# the fields the streaming validator checks are present and non-empty; full
# type/range validation is intentionally left to the ingestion-service's
# Pydantic layer at write time — the streaming layer's job is to catch
# malformed/incomplete events flowing through Kafka, not to re-implement the
# entire contract.
REQUIRED_PAYLOAD_FIELDS: dict[str, tuple[str, ...]] = {
    "orders": ("order_id", "customer_id", "product_id", "quantity", "unit_price"),
    "payments": ("payment_id", "order_id", "customer_id", "amount", "status"),
    "users": ("user_id", "action"),
    "products": ("product_id", "sku", "name", "category", "price"),
    "system": ("service_name", "status"),
}

# event_type -> numeric field on the payload used for revenue-style
# aggregation. None means "not revenue-bearing".
REVENUE_FIELD_BY_EVENT_TYPE: dict[str, str | None] = {
    "order.created": "unit_price",
    "order.updated": None,
    "payment.authorized": "amount",
    "payment.captured": "amount",
    "payment.failed": None,
}
