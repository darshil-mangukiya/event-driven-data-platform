"""Publish a bounded, deterministic-shape event-time experiment through ingestion."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone

import httpx


def _event(event_id: str, event_time: datetime, *, order_id: str) -> dict:
    return {
        "event_id": event_id,
        "idempotency_key": event_id,
        "tenant_id": "tenant_demo",
        "event_type": "order.created",
        "event_timestamp": event_time.isoformat(),
        "payload_version": 1,
        "source_service": "streaming-runtime-experiment",
        "trace_id": f"trace-{event_id}",
        "payload": {
            "order_id": order_id,
            "customer_id": "cust_stream_runtime",
            "product_id": "prod_stream_runtime",
            "quantity": 1,
            "unit_price": 10.0,
            "discount_amount": 0.0,
            "status": "created",
            "channel": "runtime-test",
            "region": "na",
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8001")
    args = parser.parse_args()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    events = [
        _event("stream-on-time", now, order_id="ord_stream_on_time"),
        _event("stream-late-accepted", now - timedelta(minutes=2), order_id="ord_stream_late_accepted"),
        _event("stream-late-rejected", now - timedelta(minutes=12), order_id="ord_stream_late_rejected"),
        _event("stream-out-of-order", now - timedelta(seconds=30), order_id="ord_stream_out_of_order"),
        _event("stream-duplicate", now - timedelta(seconds=10), order_id="ord_stream_duplicate"),
        _event("stream-duplicate", now - timedelta(seconds=10), order_id="ord_stream_duplicate"),
        # Advancing event time closes the earlier five-minute windows.
        _event("stream-watermark-advance", now + timedelta(minutes=11), order_id="ord_stream_watermark_advance"),
    ]
    with httpx.Client(timeout=30) as client:
        response = client.post(
            f"{args.base_url}/events/batch",
            headers={"X-Tenant-ID": "tenant_demo"},
            json={"events": events},
        )
        response.raise_for_status()
        result = response.json()
    print(json.dumps({"experiment_started_at": now.isoformat(), "events": events, "response": result}, sort_keys=True))


if __name__ == "__main__":
    main()
