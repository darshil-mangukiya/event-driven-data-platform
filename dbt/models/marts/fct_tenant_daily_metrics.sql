with daily as (
    select *
    from {{ source('platform', 'tenant_metrics_daily') }}
),

payments as (
    select
        tenant_id,
        payment_date as metric_date,
        count(*) filter (where status in ('captured', 'authorized')) as successful_payments,
        count(*) filter (where status = 'failed') as failed_payments
    from {{ ref('stg_payments') }}
    group by 1, 2
)

select
    d.tenant_id,
    d.metric_date,
    d.gross_revenue,
    d.net_revenue,
    d.order_count,
    d.units_sold,
    d.new_users,
    d.active_users,
    d.churn_signal_count,
    d.payment_success_count,
    d.payment_failure_count,
    coalesce(p.successful_payments, 0) as observed_successful_payments,
    coalesce(p.failed_payments, 0) as observed_failed_payments,
    d.marketing_spend,
    d.marketing_attributed_revenue,
    round(d.net_revenue / nullif(d.order_count, 0), 2) as average_order_value,
    round(d.payment_failure_count::numeric / nullif(d.payment_success_count + d.payment_failure_count, 0), 4)
        as payment_failure_rate,
    round(d.churn_signal_count::numeric / nullif(d.active_users, 0), 4) as churn_signal_rate,
    d.events_processed,
    d.updated_at
from daily d
left join payments p
    on p.tenant_id = d.tenant_id
    and p.metric_date = d.metric_date

