import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: Number(__ENV.VUS || 5),
  duration: __ENV.DURATION || "1m",
  thresholds: {
    http_req_failed: ["rate<0.02"],
    http_req_duration: ["p(95)<500"],
  },
};

const BASE_URL = __ENV.BASE_URL || "http://localhost:8001";
const TENANT_ID = __ENV.TENANT_ID || "tenant_demo";

function eventPayload(index) {
  return {
    tenant_id: TENANT_ID,
    event_type: "order.created",
    source_service: "k6-load-test",
    idempotency_key: `k6-${__VU}-${__ITER}-${index}`,
    payload: {
      order_id: `ord_k6_${__VU}_${__ITER}_${index}`,
      customer_id: `cust_${Math.floor(Math.random() * 10000)}`,
      product_id: `prod_${Math.floor(Math.random() * 500)}`,
      quantity: 1 + Math.floor(Math.random() * 4),
      unit_price: 25 + Math.random() * 300,
      discount_amount: 0,
      status: "created",
      channel: "web",
      marketing_campaign_id: "k6-local",
      region: "na",
    },
  };
}

export default function () {
  const batchSize = Number(__ENV.BATCH_SIZE || 25);
  const events = Array.from({ length: batchSize }, (_, index) => eventPayload(index));
  const response = http.post(
    `${BASE_URL}/events/batch`,
    JSON.stringify({ events }),
    {
      headers: {
        "Content-Type": "application/json",
        "X-Tenant-ID": TENANT_ID,
        "X-User-ID": "k6-load-test",
      },
    },
  );

  check(response, {
    "accepted batch": (res) => res.status === 202,
    "has accepted count": (res) => Number(res.json("accepted")) >= 0,
  });
  sleep(1);
}
