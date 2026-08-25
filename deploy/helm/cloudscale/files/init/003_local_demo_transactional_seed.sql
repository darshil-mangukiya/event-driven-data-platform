-- Local demo transactional seed data.
--
-- database/init/002_seed.sql seeded tenant_config/tenant_users/
-- tenant_products/service_health_metrics but left raw_events,
-- processed_orders, processed_payments, and processed_user_sessions
-- completely empty — only pre-aggregated tenant_metrics_daily rows
-- existed, hand-typed with no underlying transaction detail. That's a
-- real gap: it meant a fresh `docker compose up` had reconciliation
-- checks always fail (the "recomputed" side was 0
-- against every real, processed-table-backed check), and
-- /metrics/product_performance, /metrics/marketing_roi, and
-- /metrics/event_throughput — all of which query processed_orders or
-- raw_events directly, not tenant_metrics_daily — showed nothing at all.
--
-- This file seeds realistic per-order/per-payment/per-session rows using
-- the same tenant behavior shape as
-- scripts/generate_synthetic_events_v2.py's TENANT_BEHAVIORS (order
-- volume, average order value, payment failure rate, region, campaign
-- mix), for the same two-day window database/init/002_seed.sql
-- previously hand-typed aggregates for, then *derives*
-- tenant_metrics_daily from this data using the same aggregation shape as
-- scripts/backfill_metrics.py::build_daily_metrics_insert_sql() — so the
-- serving-layer numbers are consistent with their source data by
-- construction, not by two independently-maintained hand-typed copies.
--
-- Not deterministic (uses random()) — each fresh `docker compose up -v`
-- gets slightly different demo numbers, which is fine for local demo data
-- and intentionally different from scripts/generate_synthetic_events_v2.py's
-- --seed flag (that one is used for reproducible test fixtures; this one
-- is init-time seed data, regenerated fresh on every clean volume).

-- ---------------------------------------------------------------------
-- processed_orders
-- ---------------------------------------------------------------------

with demo_days(day_offset) as (values (2), (1)),
tenant_targets(tenant_id, region, currency, order_count, min_price, max_price, campaign_rate) as (
    values
        ('tenant_demo', 'us', 'USD', 150, 40.0, 160.0, 0.60),
        ('tenant_enterprise', 'us', 'USD', 90, 180.0, 440.0, 0.45),
        ('tenant_marketplace', 'eu', 'GBP', 200, 25.0, 100.0, 0.55)
),
seeded_orders as (
    select
        t.tenant_id,
        t.region,
        t.currency,
        t.campaign_rate,
        d.day_offset,
        s as seq,
        (1 + floor(random() * 4))::int as quantity,
        round((t.min_price + random() * (t.max_price - t.min_price))::numeric, 2) as unit_price,
        (array[0, 0, 0, 5, 10, 25])[1 + floor(random() * 6)::int]::numeric as discount_amount
    from tenant_targets t
    cross join demo_days d
    cross join lateral generate_series(1, t.order_count) as s
)
insert into processed_orders (
    tenant_id, event_id, order_id, customer_id, product_id, quantity, unit_price,
    discount_amount, gross_revenue, net_revenue, currency, status, channel,
    marketing_campaign_id, region, event_timestamp
)
select
    tenant_id,
    'seed-order-' || tenant_id || '-' || day_offset || '-' || seq,
    'ord_seed_' || tenant_id || '_' || day_offset || '_' || seq,
    'cust_' || tenant_id || '_' || lpad(((seq % 480) + 1)::text, 5, '0'),
    (select product_id from tenant_products p where p.tenant_id = seeded_orders.tenant_id order by product_id limit 1 offset (seq % greatest((select count(*) from tenant_products p2 where p2.tenant_id = seeded_orders.tenant_id), 1))),
    quantity,
    unit_price,
    discount_amount,
    round(quantity * unit_price, 2),
    round(greatest(quantity * unit_price - discount_amount, 0), 2),
    currency,
    'created',
    (array['web', 'mobile', 'partner', 'sales-assisted'])[1 + floor(random() * 4)::int],
    case when random() < campaign_rate then (array['paid-search', 'lifecycle', 'affiliate'])[1 + floor(random() * 3)::int] else null end,
    region,
    (current_date - (day_offset || ' days')::interval) + (floor(random() * 86400) || ' seconds')::interval
from seeded_orders
on conflict (tenant_id, order_id) do nothing;

-- ---------------------------------------------------------------------
-- raw_events (bronze layer) mirroring the orders above — same event_id,
-- so event_throughput's raw_event_count subquery (services/analytics-
-- service/app/repository.py) and data-quality freshness/required-fields
-- checks (scripts/run_data_quality_checks.py) see real, correlated data
-- instead of an empty table.
-- ---------------------------------------------------------------------

insert into raw_events (
    event_id, tenant_id, event_type, event_timestamp, source_service,
    payload_version, payload, trace_id
)
select
    o.event_id,
    o.tenant_id,
    'order.created',
    o.event_timestamp,
    'local-demo-seed',
    1,
    jsonb_build_object(
        'order_id', o.order_id, 'customer_id', o.customer_id, 'product_id', o.product_id,
        'quantity', o.quantity, 'unit_price', o.unit_price, 'discount_amount', o.discount_amount,
        'currency', o.currency, 'status', o.status, 'channel', o.channel,
        'marketing_campaign_id', o.marketing_campaign_id, 'region', o.region
    ),
    o.event_id
from processed_orders o
where o.event_id like 'seed-order-%'
on conflict (event_id) do nothing;

-- ---------------------------------------------------------------------
-- processed_payments — roughly one payment per order, sampling each
-- tenant's payment_failure_rate (matching generate_synthetic_events_v2.py's
-- TenantBehavior.payment_failure_rate values).
-- ---------------------------------------------------------------------

with tenant_failure_rates(tenant_id, failure_rate) as (
    values
        ('tenant_demo', 0.035),
        ('tenant_enterprise', 0.018),
        ('tenant_marketplace', 0.055)
),
seeded_payments as (
    select
        o.tenant_id,
        o.order_id,
        o.customer_id,
        o.net_revenue,
        o.currency,
        o.event_timestamp,
        case when random() < f.failure_rate then 'failed' else 'captured' end as status
    from processed_orders o
    join tenant_failure_rates f on f.tenant_id = o.tenant_id
    where o.event_id like 'seed-order-%'
)
insert into processed_payments (
    tenant_id, event_id, payment_id, order_id, customer_id, amount, currency,
    status, payment_method, failure_code, risk_score, event_timestamp
)
select
    tenant_id,
    'seed-payment-' || order_id,
    'pay_' || order_id,
    order_id,
    customer_id,
    net_revenue,
    currency,
    status,
    (array['card', 'wallet', 'ach'])[1 + floor(random() * 3)::int],
    case when status = 'failed' then (array['insufficient_funds', 'processor_timeout', 'risk_block'])[1 + floor(random() * 3)::int] else null end,
    case when status = 'failed' then round((0.45 + random() * 0.54)::numeric, 3) else round((0.02 + random() * 0.63)::numeric, 3) end,
    -- Clamped to the same calendar day as the order: a naive
    -- `event_timestamp + interval '2 minutes'` can cross midnight for
    -- orders placed in the last two minutes of a day, silently pushing
    -- that payment's metric_date one day later than its order's —
    -- producing a spurious extra tenant_metrics_daily row with a
    -- mismatched, near-empty aggregate. Found by actually running this
    -- seed against a live database and inspecting the derived
    -- tenant_metrics_daily output, not by inspection alone.
    least(event_timestamp + interval '2 minutes', date_trunc('day', event_timestamp) + interval '23:59:59')
from seeded_payments
on conflict (tenant_id, payment_id) do nothing;

-- High-risk failed payments produce an alert signal — mirrors the real
-- processing-service behavior (services/processing-service/app/processors.py,
-- tests/test_processing_logic.py::test_high_risk_failed_payment_creates_alert_signal)
-- so the ops console / demo dashboard Incidents section isn't empty by default.
-- Capped per tenant (not one global limit) so every tenant is represented
-- rather than one high-failure-rate tenant crowding out the others.
insert into alerts (tenant_id, alert_type, severity, status, message, source_event_id, created_at)
select tenant_id, alert_type, severity, status, message, source_event_id, created_at
from (
    select
        tenant_id,
        'payment_failure' as alert_type,
        'high' as severity,
        'open' as status,
        'High-risk failed payment for order ' || order_id as message,
        event_id as source_event_id,
        event_timestamp as created_at,
        row_number() over (partition by tenant_id order by event_timestamp desc) as rn
    from processed_payments
    where event_id like 'seed-payment-%'
      and status = 'failed'
      and risk_score >= 0.45
) ranked
where rn <= 5;

-- ---------------------------------------------------------------------
-- processed_user_sessions
-- ---------------------------------------------------------------------

with demo_days(day_offset) as (values (2), (1)),
tenant_activity(tenant_id, active_user_count, new_user_rate, churn_rate, plan) as (
    values
        ('tenant_demo', 400, 0.08, 0.025, 'growth'),
        ('tenant_enterprise', 260, 0.05, 0.012, 'enterprise'),
        ('tenant_marketplace', 500, 0.10, 0.040, 'growth')
),
seeded_sessions as (
    select
        t.tenant_id,
        t.plan,
        d.day_offset,
        s as seq,
        'user_' || t.tenant_id || '_' || lpad(s::text, 5, '0') as user_id,
        random() < t.new_user_rate as is_new_user,
        random() < t.churn_rate as is_churn_signal
    from tenant_activity t
    cross join demo_days d
    cross join lateral generate_series(1, t.active_user_count) as s
)
insert into processed_user_sessions (
    event_id, tenant_id, user_id, session_id, action, page, referrer,
    duration_seconds, plan, marketing_campaign_id, event_timestamp
)
select
    'seed-session-' || tenant_id || '-' || day_offset || '-' || seq,
    tenant_id,
    user_id,
    'sess_' || tenant_id || '_' || day_offset || '_' || seq,
    case
        when is_churn_signal then 'churn_signal'
        when is_new_user then 'signed_up'
        else (array['page_view', 'checkout_started', 'cart_updated', 'feature_used', 'support_viewed'])[1 + floor(random() * 5)::int]
    end,
    (array['/pricing', '/checkout', '/dashboard', '/products', '/billing'])[1 + floor(random() * 5)::int],
    (array['organic', 'paid', 'email', 'direct'])[1 + floor(random() * 4)::int],
    (2 + floor(random() * 898))::int,
    plan,
    case when random() < 0.4 then (array['paid-search', 'lifecycle', 'affiliate'])[1 + floor(random() * 3)::int] else null end,
    (current_date - (day_offset || ' days')::interval) + (floor(random() * 86400) || ' seconds')::interval
from seeded_sessions
on conflict (event_id) do nothing;

-- ---------------------------------------------------------------------
-- tenant_metrics_daily — DERIVED from the seed data above, using the
-- same aggregation shape as
-- scripts/backfill_metrics.py::build_daily_metrics_insert_sql() (see that
-- file for the parametrized, per-tenant/per-range version this mirrors).
-- Consistent by construction: this is not a second, independently
-- hand-typed copy of the numbers above.
-- ---------------------------------------------------------------------

with orders_agg as (
    select
        tenant_id,
        event_timestamp::date as metric_date,
        sum(gross_revenue) as gross_revenue,
        sum(net_revenue) as net_revenue,
        count(*) as order_count,
        sum(quantity) as units_sold,
        sum(case when marketing_campaign_id is not null then 3.50 else 0 end) as marketing_spend,
        sum(case when marketing_campaign_id is not null then net_revenue else 0 end) as marketing_attributed_revenue,
        count(*) as order_events
    from processed_orders
    where event_id like 'seed-order-%'
    group by 1, 2
),
payments_agg as (
    select
        tenant_id,
        event_timestamp::date as metric_date,
        count(*) filter (where status in ('authorized', 'captured')) as payment_success_count,
        count(*) filter (where status = 'failed') as payment_failure_count,
        count(*) as payment_events
    from processed_payments
    where event_id like 'seed-payment-%'
    group by 1, 2
),
users_agg as (
    select
        tenant_id,
        event_timestamp::date as metric_date,
        count(distinct user_id) filter (where action = 'signed_up') as new_users,
        count(distinct user_id) as active_users,
        count(*) filter (where action in ('churn_signal', 'cancel_intent')) as churn_signal_count,
        count(*) as user_events
    from processed_user_sessions
    where event_id like 'seed-session-%'
    group by 1, 2
)
insert into tenant_metrics_daily (
    tenant_id, metric_date, gross_revenue, net_revenue, order_count, units_sold,
    new_users, active_users, churn_signal_count, payment_success_count,
    payment_failure_count, marketing_spend, marketing_attributed_revenue,
    events_processed, updated_at
)
select
    coalesce(o.tenant_id, p.tenant_id, u.tenant_id),
    coalesce(o.metric_date, p.metric_date, u.metric_date),
    coalesce(o.gross_revenue, 0),
    coalesce(o.net_revenue, 0),
    coalesce(o.order_count, 0),
    coalesce(o.units_sold, 0),
    coalesce(u.new_users, 0),
    coalesce(u.active_users, 0),
    coalesce(u.churn_signal_count, 0),
    coalesce(p.payment_success_count, 0),
    coalesce(p.payment_failure_count, 0),
    coalesce(o.marketing_spend, 0),
    coalesce(o.marketing_attributed_revenue, 0),
    coalesce(o.order_events, 0) + coalesce(p.payment_events, 0) + coalesce(u.user_events, 0),
    now()
from orders_agg o
full outer join payments_agg p on p.tenant_id = o.tenant_id and p.metric_date = o.metric_date
full outer join users_agg u on u.tenant_id = coalesce(o.tenant_id, p.tenant_id) and u.metric_date = coalesce(o.metric_date, p.metric_date)
on conflict (tenant_id, metric_date) do update set
    gross_revenue = excluded.gross_revenue,
    net_revenue = excluded.net_revenue,
    order_count = excluded.order_count,
    units_sold = excluded.units_sold,
    new_users = excluded.new_users,
    active_users = excluded.active_users,
    churn_signal_count = excluded.churn_signal_count,
    payment_success_count = excluded.payment_success_count,
    payment_failure_count = excluded.payment_failure_count,
    marketing_spend = excluded.marketing_spend,
    marketing_attributed_revenue = excluded.marketing_attributed_revenue,
    events_processed = excluded.events_processed,
    updated_at = now();
