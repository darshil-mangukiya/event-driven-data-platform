"""Contract validation on top of the parsed envelope columns.

``event_parser.py`` already flags structural problems (malformed JSON,
missing envelope fields, unparseable timestamps). This module adds the
remaining contract checks that require domain knowledge:

* unknown/unsupported ``event_type``
* unsupported ``payload_version``
* missing ``tenant_id`` (redundant with the envelope check, called out
  explicitly because tenant isolation is a hard platform requirement)
* missing required payload fields for the event's domain

Every record ends up with a ``validation_status`` of ``"valid"`` or
``"invalid"`` and, when invalid, a ``validation_reason`` code. Nothing is
dropped here — callers use :func:`split_valid_invalid` to route the two
halves to different sinks (serving pipeline vs. DLQ).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.streaming.config import SUPPORTED_PAYLOAD_VERSIONS
from spark.streaming.schemas import EVENT_TYPE_DOMAIN, KNOWN_EVENT_TYPES, REQUIRED_PAYLOAD_FIELDS

VALID = "valid"
INVALID = "invalid"

REASON_MALFORMED_JSON = "malformed_json"
REASON_MISSING_FIELDS = "missing_required_envelope_fields"
REASON_INVALID_TIMESTAMP = "invalid_event_timestamp"
REASON_MISSING_TENANT = "missing_tenant_id"
REASON_UNKNOWN_EVENT_TYPE = "unknown_event_type"
REASON_UNSUPPORTED_VERSION = "unsupported_payload_version"
REASON_MISSING_PAYLOAD_FIELDS = "missing_required_payload_fields"


def validate_events(df: DataFrame) -> DataFrame:
    """Add ``event_domain``, ``validation_status``, ``validation_reason``."""
    # A literal create_map looks compact, but Spark 3.5 can duplicate its
    # key/value children while optimizing several stateful queries derived
    # from the same stream. Constant folding then aborts with a duplicated
    # map-key error even though the source mapping itself is unique. A CASE
    # expression is equally deterministic and survives multi-query planning.
    domain_expr = F.lit(None).cast("string")
    for event_type, domain in reversed(tuple(EVENT_TYPE_DOMAIN.items())):
        domain_expr = F.when(F.col("event_type") == event_type, F.lit(domain)).otherwise(
            domain_expr
        )
    df = df.withColumn("event_domain", domain_expr)

    df = df.withColumn(
        "missing_payload_fields",
        F.when(F.col("event_domain").isNotNull(), _missing_payload_fields_expr(df)).otherwise(
            F.array()
        ),
    )

    df = df.withColumn(
        "validation_reason",
        F.when(F.col("is_malformed_json"), F.lit(REASON_MALFORMED_JSON))
        .when(F.size(F.col("missing_fields")) > 0, F.lit(REASON_MISSING_FIELDS))
        .when(F.col("has_invalid_timestamp"), F.lit(REASON_INVALID_TIMESTAMP))
        .when(F.col("tenant_id").isNull() | (F.trim(F.col("tenant_id")) == ""), F.lit(REASON_MISSING_TENANT))
        .when(~F.col("event_type").isin(*KNOWN_EVENT_TYPES), F.lit(REASON_UNKNOWN_EVENT_TYPE))
        .when(
            ~F.col("payload_version").isin(*SUPPORTED_PAYLOAD_VERSIONS),
            F.lit(REASON_UNSUPPORTED_VERSION),
        )
        .when(F.size(F.col("missing_payload_fields")) > 0, F.lit(REASON_MISSING_PAYLOAD_FIELDS))
        .otherwise(F.lit(None).cast("string")),
    )

    df = df.withColumn(
        "validation_status",
        F.when(F.col("validation_reason").isNull(), F.lit(VALID)).otherwise(F.lit(INVALID)),
    )
    return df


def _missing_payload_fields_expr(df: DataFrame):
    """Build a per-row 'which required payload fields are missing' array.

    Implemented with nested ``when`` over ``get_json_object`` lookups so it
    works for any event_domain without a Python-side loop over rows.
    """
    all_domains = sorted(REQUIRED_PAYLOAD_FIELDS)
    expr = F.array()
    for domain in all_domains:
        fields = REQUIRED_PAYLOAD_FIELDS[domain]
        missing_for_domain = F.array(
            *[
                F.when(
                    F.get_json_object(F.col("payload_json"), f"$.{field_name}").isNull(),
                    F.lit(field_name),
                )
                for field_name in fields
            ]
        )
        missing_for_domain = F.array_except(missing_for_domain, F.array(F.lit(None).cast("string")))
        expr = F.when(F.col("event_domain") == domain, missing_for_domain).otherwise(expr)
    return expr


def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = df.filter(F.col("validation_status") == VALID)
    invalid = df.filter(F.col("validation_status") == INVALID)
    return valid, invalid
