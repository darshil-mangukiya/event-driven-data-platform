-- Parameterized source SQL mirrored by scripts/backfill_metrics.py.
-- Bindings: $1 tenant_id, $2 start_date, $3 end_date.
-- Operational pattern:
--   1. Dry-run the CLI and review the bounded tenant/date plan.
--   2. Delete only the tenant/date slice from tenant_metrics_daily.
--   3. Recompute that serving slice from processed source tables.
--   4. Let Redis TTL expire or force flush urgent tenant metric keys.

delete from tenant_metrics_daily
where tenant_id = $1
  and metric_date between $2 and $3;

insert into tenant_metrics_daily (
    tenant_id, metric_date, gross_revenue, net_revenue, order_count, units_sold,
    new_users, active_users, churn_signal_count, payment_success_count,
    payment_failure_count, marketing_spend, marketing_attributed_revenue,
    events_processed, updated_at
)
with dates as (
    select generate_series($2::date, $3::date, interval '1 day')::date as metric_date
),
orders as (
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
    where tenant_id = $1
      and event_timestamp::date between $2 and $3
    group by 1, 2
),
payments as (
    select
        tenant_id,
        event_timestamp::date as metric_date,
        count(*) filter (where status in ('authorized', 'captured')) as payment_success_count,
        count(*) filter (where status = 'failed') as payment_failure_count,
        count(*) as payment_events
    from processed_payments
    where tenant_id = $1
      and event_timestamp::date between $2 and $3
    group by 1, 2
),
users as (
    select
        tenant_id,
        event_timestamp::date as metric_date,
        count(distinct user_id) filter (where action = 'signed_up') as new_users,
        count(distinct user_id) as active_users,
        count(*) filter (where action in ('churn_signal', 'cancel_intent')) as churn_signal_count,
        count(*) as user_events
    from processed_user_sessions
    where tenant_id = $1
      and event_timestamp::date between $2 and $3
    group by 1, 2
)
select
    $1 as tenant_id,
    d.metric_date,
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
from dates d
left join orders o on o.metric_date = d.metric_date and o.tenant_id = $1
left join payments p on p.metric_date = d.metric_date and p.tenant_id = $1
left join users u on u.metric_date = d.metric_date and u.tenant_id = $1;
