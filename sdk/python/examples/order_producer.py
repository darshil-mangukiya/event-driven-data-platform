from __future__ import annotations

from platform_producer import PlatformProducerClient, ProducerEvent, derive_business_idempotency_key


def main() -> None:
    event = ProducerEvent(
        tenant_id="tenant_demo",
        event_type="order.created",
        source_service="checkout-api",
        idempotency_key=derive_business_idempotency_key(
            source="checkout-api",
            entity_id="ord_sdk_1001",
            action="created",
        ),
        payload={
            "order_id": "ord_sdk_1001",
            "customer_id": "cust_sdk_1001",
            "product_id": "prod_001",
            "quantity": 2,
            "unit_price": 49.0,
            "discount_amount": 5.0,
            "status": "created",
            "channel": "web",
        },
    )
    with PlatformProducerClient(base_url="http://localhost:8001", tenant_id="tenant_demo") as client:
        print(client.publish(event))


if __name__ == "__main__":
    main()
