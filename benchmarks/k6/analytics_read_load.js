import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: Number(__ENV.VUS || 10),
  duration: __ENV.DURATION || "1m",
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<300"],
  },
};

const BASE_URL = __ENV.BASE_URL || "http://localhost:8003";
const TENANT_ID = __ENV.TENANT_ID || "tenant_demo";

export default function () {
  const response = http.get(`${BASE_URL}/metrics/revenue?tenant_id=${TENANT_ID}&limit=7`, {
    headers: {
      "X-Tenant-ID": TENANT_ID,
      "X-User-ID": "k6-analytics-reader",
    },
  });

  check(response, {
    "revenue api ok": (res) => res.status === 200,
    "tenant scoped": (res) => res.json("tenant_id") === TENANT_ID,
  });
  sleep(1);
}
