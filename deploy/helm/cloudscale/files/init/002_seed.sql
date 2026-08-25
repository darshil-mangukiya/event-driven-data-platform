insert into tenant_config (tenant_id, tenant_name, plan, region, config)
values
    ('tenant_demo', 'Demo Commerce', 'growth', 'us', '{"timezone":"America/Los_Angeles","currency":"USD","retention_days":365}'),
    ('tenant_enterprise', 'Northstar SaaS', 'enterprise', 'us', '{"timezone":"America/New_York","currency":"USD","retention_days":730}'),
    ('tenant_marketplace', 'MarketHub', 'growth', 'eu', '{"timezone":"Europe/London","currency":"GBP","retention_days":365}')
on conflict (tenant_id) do nothing;

insert into tenant_users (tenant_id, user_id, email, role)
values
    ('tenant_demo', 'analyst_demo', 'analyst@demo-commerce.test', 'tenant_analyst'),
    ('tenant_enterprise', 'analyst_enterprise', 'analytics@northstar.test', 'tenant_admin'),
    ('tenant_marketplace', 'analyst_marketplace', 'ops@markethub.test', 'tenant_analyst')
on conflict (tenant_id, user_id) do nothing;

insert into tenant_products (tenant_id, product_id, sku, name, category, price, inventory_on_hand)
values
    ('tenant_demo', 'prod_001', 'PROD-001', 'Starter Kit', 'core', 49.00, 500),
    ('tenant_demo', 'prod_002', 'PROD-002', 'Pro Bundle', 'growth', 149.00, 250),
    ('tenant_enterprise', 'prod_001', 'ENT-001', 'Enterprise Seat', 'subscription', 199.00, 1000),
    ('tenant_marketplace', 'prod_001', 'MKT-001', 'Seller Boost', 'marketplace', 89.00, 300)
on conflict (tenant_id, product_id) do nothing;

-- tenant_metrics_daily is deliberately NOT hand-seeded here — see
-- database/init/003_local_demo_transactional_seed.sql, which seeds
-- realistic processed_orders/processed_payments/processed_user_sessions
-- rows and then *derives* tenant_metrics_daily from them using the same
-- aggregation shape as scripts/backfill_metrics.py. Two independently
-- hand-typed sources of the same numbers is exactly the class of
-- catalog/reality gap validation identified and corrected elsewhere; this
-- keeps the serving-layer seed data consistent with its source data by
-- construction, and means a fresh `docker compose up` reconciles cleanly
-- out of the box instead of always showing a drift on first run.

insert into service_health_metrics (
    event_id, tenant_id, service_name, status, latency_ms, error_count,
    throughput_per_minute, kafka_lag, cache_hit_rate, message, event_timestamp
)
values
    ('seed-health-analytics', 'tenant_demo', 'analytics-service', 'healthy', 38.5, 0, 1200, 3, 0.87, 'seed health sample', now()),
    ('seed-health-processing', 'tenant_demo', 'processing-service', 'healthy', 24.2, 0, 4300, 12, null, 'seed health sample', now())
on conflict (event_id) do nothing;

