-- Recompute daily order metrics from processed facts and compare them with
-- tenant_metrics_daily serving rows.
-- Bindings: $1 tenant_id, $2 start_date, $3 end_date.

with dates as (
    select generate_series($2::date, $3::date, interval '1 day')::date as metric_date
),
recomputed as (
    select
        d.metric_date,
        coalesce(sum(o.net_revenue), 0) as recomputed_net_revenue,
        coalesce(count(o.order_id), 0) as recomputed_order_count,
        coalesce(sum(o.quantity), 0) as recomputed_units_sold
    from dates d
    left join processed_orders o
      on o.tenant_id = $1
     and o.event_timestamp::date = d.metric_date
    group by 1
)
select
    $1 as tenant_id,
    r.metric_date,
    coalesce(m.net_revenue, 0) as serving_net_revenue,
    r.recomputed_net_revenue,
    coalesce(m.order_count, 0) as serving_order_count,
    r.recomputed_order_count,
    coalesce(m.units_sold, 0) as serving_units_sold,
    r.recomputed_units_sold,
    coalesce(m.events_processed, 0) as serving_events_processed
from recomputed r
left join tenant_metrics_daily m
  on m.tenant_id = $1
 and m.metric_date = r.metric_date
order by r.metric_date;
