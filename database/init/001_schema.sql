create extension if not exists pgcrypto;

create table if not exists tenant_config (
    tenant_id text primary key,
    tenant_name text not null,
    plan text not null default 'growth',
    region text not null default 'us',
    is_active boolean not null default true,
    config jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists tenant_users (
    tenant_id text not null references tenant_config(tenant_id),
    user_id text not null,
    email text not null,
    role text not null default 'tenant_analyst',
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    primary key (tenant_id, user_id)
);

create table if not exists tenant_products (
    tenant_id text not null references tenant_config(tenant_id),
    product_id text not null,
    sku text not null,
    name text not null,
    category text not null,
    price numeric(14, 2) not null,
    inventory_on_hand integer not null default 0,
    active boolean not null default true,
    last_event_id text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (tenant_id, product_id)
);

create table if not exists raw_events (
    event_id text primary key,
    tenant_id text not null references tenant_config(tenant_id),
    event_type text not null,
    event_timestamp timestamptz not null,
    source_service text not null,
    payload_version integer not null,
    payload jsonb not null,
    trace_id text not null,
    correlation_id text,
    causation_id text,
    idempotency_key text,
    ingested_at timestamptz not null default now()
);

create table if not exists event_outbox (
    outbox_id uuid primary key default gen_random_uuid(),
    aggregate_id text not null,
    tenant_id text not null references tenant_config(tenant_id),
    event_id text not null unique,
    event_type text not null,
    source_service text not null,
    payload_version integer not null default 1,
    payload jsonb not null,
    idempotency_key text,
    status text not null default 'pending',
    attempts integer not null default 0,
    available_at timestamptz not null default now(),
    locked_at timestamptz,
    published_at timestamptz,
    error_message text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists event_inbox (
    consumer_name text not null,
    event_id text not null,
    tenant_id text not null references tenant_config(tenant_id),
    event_type text not null,
    trace_id text not null,
    status text not null default 'processing',
    received_at timestamptz not null default now(),
    processed_at timestamptz,
    error_message text,
    primary key (consumer_name, event_id)
);

create table if not exists processed_orders (
    tenant_id text not null references tenant_config(tenant_id),
    event_id text not null unique,
    order_id text not null,
    customer_id text not null,
    product_id text not null,
    quantity integer not null,
    unit_price numeric(14, 2) not null,
    discount_amount numeric(14, 2) not null default 0,
    gross_revenue numeric(14, 2) not null,
    net_revenue numeric(14, 2) not null,
    currency text not null default 'USD',
    status text not null,
    channel text not null,
    marketing_campaign_id text,
    region text,
    event_timestamp timestamptz not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (tenant_id, order_id)
);

create table if not exists processed_payments (
    tenant_id text not null references tenant_config(tenant_id),
    event_id text not null unique,
    payment_id text not null,
    order_id text not null,
    customer_id text not null,
    amount numeric(14, 2) not null,
    currency text not null default 'USD',
    status text not null,
    payment_method text not null,
    failure_code text,
    risk_score numeric(5, 4),
    event_timestamp timestamptz not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (tenant_id, payment_id)
);

create table if not exists processed_user_sessions (
    event_id text primary key,
    tenant_id text not null references tenant_config(tenant_id),
    user_id text not null,
    session_id text not null,
    action text not null,
    page text,
    referrer text,
    duration_seconds integer not null default 0,
    plan text,
    marketing_campaign_id text,
    event_timestamp timestamptz not null,
    created_at timestamptz not null default now()
);

create table if not exists tenant_metrics_hourly (
    tenant_id text not null references tenant_config(tenant_id),
    metric_hour timestamptz not null,
    gross_revenue numeric(18, 2) not null default 0,
    net_revenue numeric(18, 2) not null default 0,
    order_count integer not null default 0,
    units_sold integer not null default 0,
    new_users integer not null default 0,
    active_users integer not null default 0,
    churn_signal_count integer not null default 0,
    payment_success_count integer not null default 0,
    payment_failure_count integer not null default 0,
    marketing_spend numeric(18, 2) not null default 0,
    marketing_attributed_revenue numeric(18, 2) not null default 0,
    events_processed integer not null default 0,
    updated_at timestamptz not null default now(),
    primary key (tenant_id, metric_hour)
);

create table if not exists tenant_metrics_daily (
    tenant_id text not null references tenant_config(tenant_id),
    metric_date date not null,
    gross_revenue numeric(18, 2) not null default 0,
    net_revenue numeric(18, 2) not null default 0,
    order_count integer not null default 0,
    units_sold integer not null default 0,
    new_users integer not null default 0,
    active_users integer not null default 0,
    churn_signal_count integer not null default 0,
    payment_success_count integer not null default 0,
    payment_failure_count integer not null default 0,
    marketing_spend numeric(18, 2) not null default 0,
    marketing_attributed_revenue numeric(18, 2) not null default 0,
    events_processed integer not null default 0,
    updated_at timestamptz not null default now(),
    primary key (tenant_id, metric_date)
);

create table if not exists fraud_or_risk_events (
    risk_event_id uuid primary key default gen_random_uuid(),
    tenant_id text not null references tenant_config(tenant_id),
    event_id text not null,
    risk_type text not null,
    risk_score numeric(5, 4),
    payload jsonb not null,
    created_at timestamptz not null default now()
);

create table if not exists alerts (
    alert_id uuid primary key default gen_random_uuid(),
    tenant_id text not null references tenant_config(tenant_id),
    alert_type text not null,
    severity text not null,
    status text not null default 'open',
    message text not null,
    source_event_id text,
    created_at timestamptz not null default now(),
    acknowledged_at timestamptz
);

create table if not exists service_health_metrics (
    event_id text primary key,
    tenant_id text not null references tenant_config(tenant_id),
    service_name text not null,
    status text not null,
    latency_ms numeric(12, 2),
    error_count integer not null default 0,
    throughput_per_minute numeric(14, 2),
    kafka_lag integer,
    cache_hit_rate numeric(6, 4),
    message text,
    event_timestamp timestamptz not null,
    created_at timestamptz not null default now()
);

create table if not exists pipeline_run_log (
    pipeline_run_id uuid primary key default gen_random_uuid(),
    pipeline_name text not null,
    status text not null,
    records_processed integer not null default 0,
    error_message text,
    started_at timestamptz not null,
    finished_at timestamptz
);

create table if not exists pipeline_watermarks (
    pipeline_name text not null,
    tenant_id text not null references tenant_config(tenant_id),
    source_topic text not null,
    last_processed_timestamp timestamptz,
    last_processed_offset bigint,
    status text not null default 'active',
    updated_at timestamptz not null default now(),
    metadata jsonb not null default '{}'::jsonb,
    primary key (pipeline_name, tenant_id, source_topic)
);

create table if not exists api_usage_log (
    usage_id uuid primary key default gen_random_uuid(),
    tenant_id text not null,
    user_id text,
    endpoint text not null,
    status_code integer not null,
    latency_ms numeric(12, 2) not null,
    cache_status text,
    role text,
    trace_id text,
    requested_at timestamptz not null default now()
);

create table if not exists dlq_replay_audit (
    replay_id uuid primary key default gen_random_uuid(),
    original_event_id text,
    tenant_id text,
    source_topic text not null,
    target_topic text,
    replay_status text not null,
    replay_reason text,
    replayed_by text not null default 'local-operator',
    replayed_at timestamptz not null default now(),
    error_message text
);

create table if not exists data_quality_check_results (
    check_result_id uuid primary key default gen_random_uuid(),
    check_name text not null,
    check_category text not null,
    tenant_id text,
    status text not null,
    severity text not null,
    observed_value numeric,
    threshold_value numeric,
    details jsonb not null default '{}'::jsonb,
    checked_at timestamptz not null default now()
);

create table if not exists data_quality_score_daily (
    tenant_id text not null,
    score_date date not null,
    quality_score numeric(5, 2) not null,
    passed_checks integer not null,
    failed_checks integer not null,
    warning_checks integer not null,
    critical_checks integer not null,
    updated_at timestamptz not null default now(),
    primary key (tenant_id, score_date)
);

create table if not exists benchmark_run_results (
    benchmark_run_id uuid primary key default gen_random_uuid(),
    benchmark_name text not null,
    tenant_id text not null,
    target_url text not null,
    total_events integer not null,
    elapsed_seconds numeric(14, 4) not null,
    events_per_second numeric(14, 4) not null,
    failure_count integer not null,
    p50_latency_ms numeric(14, 4),
    p95_latency_ms numeric(14, 4),
    p99_latency_ms numeric(14, 4),
    max_latency_ms numeric(14, 4),
    result_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists reconciliation_audit (
    reconciliation_id uuid primary key default gen_random_uuid(),
    tenant_id text not null references tenant_config(tenant_id),
    metric_date date not null,
    check_name text not null,
    status text not null,
    revenue_delta numeric(18, 4) not null default 0,
    order_count_delta integer not null default 0,
    units_sold_delta integer not null default 0,
    details jsonb not null default '{}'::jsonb,
    checked_by text not null default 'local-operator',
    checked_at timestamptz not null default now()
);

create table if not exists lineage_events (
    lineage_event_id uuid primary key default gen_random_uuid(),
    event_type text not null,
    job_name text not null,
    run_id text not null,
    tenant_id text,
    input_datasets jsonb not null default '[]'::jsonb,
    output_datasets jsonb not null default '[]'::jsonb,
    status text not null,
    event_timestamp timestamptz not null default now(),
    metadata jsonb not null default '{}'::jsonb
);

create table if not exists privacy_erasure_requests (
    request_id uuid primary key default gen_random_uuid(),
    tenant_id text not null references tenant_config(tenant_id),
    subject_id text not null,
    subject_type text not null default 'user',
    status text not null default 'requested',
    requested_by text not null,
    reason text,
    requested_at timestamptz not null default now(),
    completed_at timestamptz,
    evidence jsonb not null default '{}'::jsonb
);

create table if not exists data_retention_policies (
    policy_id uuid primary key default gen_random_uuid(),
    table_name text not null,
    tenant_plan text not null default 'all',
    retention_days integer not null,
    action text not null default 'archive_then_delete',
    is_active boolean not null default true,
    updated_at timestamptz not null default now(),
    unique (table_name, tenant_plan)
);

insert into data_retention_policies (table_name, tenant_plan, retention_days, action)
values
    ('raw_events', 'all', 180, 'archive_then_delete'),
    ('api_usage_log', 'all', 90, 'delete'),
    ('service_health_metrics', 'all', 90, 'archive_then_delete'),
    ('data_quality_check_results', 'all', 365, 'archive_then_delete')
on conflict (table_name, tenant_plan) do nothing;

create index if not exists idx_raw_events_tenant_timestamp on raw_events (tenant_id, event_timestamp desc);
create index if not exists idx_raw_events_type_timestamp on raw_events (event_type, event_timestamp desc);
create index if not exists idx_raw_events_traceability on raw_events (tenant_id, correlation_id, event_timestamp desc);
create index if not exists idx_raw_events_idempotency on raw_events (tenant_id, idempotency_key);
create index if not exists idx_raw_events_payload_gin on raw_events using gin (payload);
create index if not exists idx_event_outbox_status_available on event_outbox (status, available_at, created_at);
create index if not exists idx_event_outbox_tenant on event_outbox (tenant_id, created_at desc);
create index if not exists idx_event_inbox_tenant_status on event_inbox (tenant_id, status, received_at desc);
create index if not exists idx_processed_orders_tenant_date on processed_orders (tenant_id, event_timestamp desc);
create index if not exists idx_processed_orders_product on processed_orders (tenant_id, product_id, event_timestamp desc);
create index if not exists idx_processed_orders_campaign on processed_orders (tenant_id, marketing_campaign_id, event_timestamp desc);
create index if not exists idx_processed_payments_status on processed_payments (tenant_id, status, event_timestamp desc);
create index if not exists idx_processed_sessions_user on processed_user_sessions (tenant_id, user_id, event_timestamp desc);
create index if not exists idx_alerts_tenant_status on alerts (tenant_id, status, created_at desc);
create index if not exists idx_service_health_tenant_time on service_health_metrics (tenant_id, event_timestamp desc);
create index if not exists idx_pipeline_run_log_time on pipeline_run_log (started_at desc);
create index if not exists idx_pipeline_watermarks_status on pipeline_watermarks (status, updated_at desc);
create index if not exists idx_api_usage_log_tenant_time on api_usage_log (tenant_id, requested_at desc);
create index if not exists idx_api_usage_log_trace on api_usage_log (trace_id);
create index if not exists idx_dlq_replay_audit_time on dlq_replay_audit (replayed_at desc);
create index if not exists idx_quality_results_time on data_quality_check_results (checked_at desc);
create index if not exists idx_quality_score_tenant_date on data_quality_score_daily (tenant_id, score_date desc);
create index if not exists idx_benchmark_results_time on benchmark_run_results (created_at desc);
create index if not exists idx_reconciliation_audit_tenant_date on reconciliation_audit (tenant_id, metric_date desc);
create index if not exists idx_lineage_events_job_time on lineage_events (job_name, event_timestamp desc);
create index if not exists idx_privacy_erasure_tenant_status on privacy_erasure_requests (tenant_id, status, requested_at desc);

create or replace view tenant_analytics_isolated as
select
    m.tenant_id,
    t.tenant_name,
    t.plan,
    m.metric_date,
    m.net_revenue,
    m.order_count,
    m.active_users,
    m.churn_signal_count,
    m.payment_success_count,
    m.payment_failure_count,
    m.events_processed
from tenant_metrics_daily m
join tenant_config t on t.tenant_id = m.tenant_id
where t.is_active = true;

-- Structured Streaming serving tables (see database/migrations/versions/0006_streaming_serving_tables.py)
create table if not exists stream_processing_runs (
    run_id uuid primary key,
    job_name text not null,
    status text not null default 'running',
    config_snapshot jsonb not null default '{}'::jsonb,
    started_at timestamptz not null default now(),
    ended_at timestamptz
);

create index if not exists idx_stream_processing_runs_job_started
    on stream_processing_runs (job_name, started_at desc);

create table if not exists stream_window_metrics (
    tenant_id text not null references tenant_config(tenant_id),
    window_start timestamptz not null,
    window_end timestamptz not null,
    event_domain text not null,
    metric_name text not null,
    metric_value double precision not null default 0,
    event_count integer not null default 0,
    batch_id bigint not null,
    updated_at timestamptz not null default now(),
    primary key (tenant_id, window_start, window_end, event_domain, metric_name)
);

create index if not exists idx_stream_window_metrics_tenant_time
    on stream_window_metrics (tenant_id, window_start desc);
create index if not exists idx_stream_window_metrics_metric
    on stream_window_metrics (metric_name, window_start desc);

create table if not exists streaming_watermarks (
    run_id uuid not null references stream_processing_runs(run_id),
    batch_id bigint not null,
    event_time_watermark timestamptz,
    recorded_at timestamptz not null default now(),
    primary key (run_id, batch_id)
);

create table if not exists streaming_checkpoint_audit (
    run_id uuid not null references stream_processing_runs(run_id),
    query_name text not null,
    batch_id bigint not null,
    checkpoint_location text not null,
    input_rows integer not null default 0,
    batch_duration_ms double precision,
    recorded_at timestamptz not null default now(),
    primary key (run_id, query_name, batch_id)
);

create table if not exists streaming_failures (
    failure_id uuid primary key,
    run_id uuid not null references stream_processing_runs(run_id),
    batch_id bigint,
    tenant_id text,
    stage text not null,
    error_message text not null,
    recorded_at timestamptz not null default now()
);

create index if not exists idx_streaming_failures_run
    on streaming_failures (run_id, recorded_at desc);

create table if not exists streaming_late_events (
    event_id text not null,
    tenant_id text not null references tenant_config(tenant_id),
    event_type text not null,
    event_domain text,
    event_timestamp timestamptz,
    ingestion_timestamp timestamptz,
    lateness_seconds bigint,
    classification text not null,
    batch_id bigint not null,
    recorded_at timestamptz not null default now(),
    primary key (tenant_id, event_id)
);

create index if not exists idx_streaming_late_events_classification
    on streaming_late_events (tenant_id, classification, recorded_at desc);

-- Runtime Schema Registry (services/schema-registry-service). Kept in
-- sync with database/migrations/versions/0007_schema_registry.py — see
-- that file's own header for why both the consolidated init SQL and the
-- incremental Alembic migration exist (this project provisions a fresh
-- local stack via docker-entrypoint-initdb.d running this file, not via
-- `alembic upgrade head`; the Alembic migration is the documented
-- incremental-upgrade reference).
create table if not exists schema_registry_subjects (
    subject text primary key,
    compatibility_mode text not null default 'BACKWARD',
    created_at timestamptz not null default now()
);

create table if not exists schema_registry_versions (
    subject text not null references schema_registry_subjects(subject),
    version integer not null,
    schema_id uuid not null default gen_random_uuid(),
    schema_json jsonb not null,
    registered_at timestamptz not null default now(),
    registered_by text,
    primary key (subject, version)
);

create index if not exists idx_schema_registry_versions_subject
    on schema_registry_versions (subject, version desc);

create table if not exists schema_registry_compatibility_checks (
    id uuid primary key default gen_random_uuid(),
    subject text not null,
    compatibility_mode text not null,
    is_compatible boolean not null,
    errors jsonb not null default '[]'::jsonb,
    dry_run boolean not null default true,
    checked_at timestamptz not null default now()
);

create index if not exists idx_schema_registry_compat_checks_subject
    on schema_registry_compatibility_checks (subject, checked_at desc);
