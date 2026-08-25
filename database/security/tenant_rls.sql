-- Runtime tenant-isolation policies for PostgreSQL serving tables. Fresh
-- database initialization applies this file automatically. It can also be
-- applied to an existing volume and is safe to run repeatedly.
-- Services must set `select set_config('app.tenant_id', '<tenant>', true);`
-- within each tenant-scoped transaction. FORCE RLS covers table-owner access;
-- tenant services connect through the NOSUPERUSER/NOBYPASSRLS role created
-- below. Cross-tenant local tools use a separate BYPASSRLS role.

create or replace function current_app_tenant_id()
returns text
language sql
stable
as $$
    select nullif(current_setting('app.tenant_id', true), '')
$$;

alter table raw_events enable row level security;
alter table raw_events force row level security;
alter table event_outbox enable row level security;
alter table event_outbox force row level security;
alter table event_inbox enable row level security;
alter table event_inbox force row level security;
alter table processed_orders enable row level security;
alter table processed_orders force row level security;
alter table processed_payments enable row level security;
alter table processed_payments force row level security;
alter table processed_user_sessions enable row level security;
alter table processed_user_sessions force row level security;
alter table tenant_metrics_hourly enable row level security;
alter table tenant_metrics_hourly force row level security;
alter table tenant_metrics_daily enable row level security;
alter table tenant_metrics_daily force row level security;
alter table alerts enable row level security;
alter table alerts force row level security;
alter table service_health_metrics enable row level security;
alter table service_health_metrics force row level security;
alter table privacy_erasure_requests enable row level security;
alter table privacy_erasure_requests force row level security;

-- Each policy is preceded by `drop policy if exists` so this script is
-- safe to run more than once against the same database or an existing volume.
-- RLS remains enabled while privileged initialization recreates each policy.
drop policy if exists tenant_raw_events on raw_events;
create policy tenant_raw_events on raw_events
    using (tenant_id = current_app_tenant_id())
    with check (tenant_id = current_app_tenant_id());
drop policy if exists tenant_event_outbox on event_outbox;
create policy tenant_event_outbox on event_outbox
    using (tenant_id = current_app_tenant_id())
    with check (tenant_id = current_app_tenant_id());
drop policy if exists tenant_event_inbox on event_inbox;
create policy tenant_event_inbox on event_inbox
    using (tenant_id = current_app_tenant_id())
    with check (tenant_id = current_app_tenant_id());
drop policy if exists tenant_processed_orders on processed_orders;
create policy tenant_processed_orders on processed_orders
    using (tenant_id = current_app_tenant_id())
    with check (tenant_id = current_app_tenant_id());
drop policy if exists tenant_processed_payments on processed_payments;
create policy tenant_processed_payments on processed_payments
    using (tenant_id = current_app_tenant_id())
    with check (tenant_id = current_app_tenant_id());
drop policy if exists tenant_processed_user_sessions on processed_user_sessions;
create policy tenant_processed_user_sessions on processed_user_sessions
    using (tenant_id = current_app_tenant_id())
    with check (tenant_id = current_app_tenant_id());
drop policy if exists tenant_metrics_hourly on tenant_metrics_hourly;
create policy tenant_metrics_hourly on tenant_metrics_hourly
    using (tenant_id = current_app_tenant_id())
    with check (tenant_id = current_app_tenant_id());
drop policy if exists tenant_metrics_daily on tenant_metrics_daily;
create policy tenant_metrics_daily on tenant_metrics_daily
    using (tenant_id = current_app_tenant_id())
    with check (tenant_id = current_app_tenant_id());
drop policy if exists tenant_alerts on alerts;
create policy tenant_alerts on alerts
    using (tenant_id = current_app_tenant_id())
    with check (tenant_id = current_app_tenant_id());
drop policy if exists tenant_service_health_metrics on service_health_metrics;
create policy tenant_service_health_metrics on service_health_metrics
    using (tenant_id = current_app_tenant_id())
    with check (tenant_id = current_app_tenant_id());
drop policy if exists tenant_privacy_erasure_requests on privacy_erasure_requests;
create policy tenant_privacy_erasure_requests on privacy_erasure_requests
    using (tenant_id = current_app_tenant_id())
    with check (tenant_id = current_app_tenant_id());

-- The dedicated non-superuser role RLS requires (see the header
-- comment's bug #2). Ordinary tenant-scoped application traffic should
-- connect as this role, not the default local `platform` superuser, for
-- RLS to provide any real protection.
do $$
begin
    if not exists (select 1 from pg_roles where rolname = 'platform_tenant_scoped') then
        create role platform_tenant_scoped with login password 'local-tenant-scoped-change-me' nosuperuser nobypassrls;
        grant select, insert, update, delete on all tables in schema public to platform_tenant_scoped;
        grant usage, select on all sequences in schema public to platform_tenant_scoped;
    end if;
end
$$;

-- A platform-admin bypass role: reflects the application-layer
-- `platform_admin` role (services/shared/platform_shared/auth.py),
-- which is allowed cross-tenant access by design (TenantPrincipal.
-- can_access() returns True unconditionally for it). At the database
-- layer this is modeled with PostgreSQL's native BYPASSRLS attribute on
-- a dedicated, still-non-superuser role — never grant BYPASSRLS to the
-- same role ordinary tenant-scoped application connections use.
do $$
begin
    if not exists (select 1 from pg_roles where rolname = 'platform_admin_bypass') then
        create role platform_admin_bypass with login password 'local-admin-bypass-change-me' nosuperuser bypassrls;
        grant select, insert, update, delete on all tables in schema public to platform_admin_bypass;
        grant usage, select on all sequences in schema public to platform_admin_bypass;
    end if;
end
$$;
