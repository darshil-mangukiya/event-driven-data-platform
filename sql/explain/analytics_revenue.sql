explain (analyze false, buffers false, costs true)
select metric_date, gross_revenue, net_revenue, order_count, units_sold,
       round(net_revenue / nullif(order_count, 0), 2) as average_order_value
from tenant_metrics_daily
where tenant_id = 'tenant_demo'
  and metric_date >= current_date - interval '30 day'
order by metric_date desc
limit 30 offset 0;

