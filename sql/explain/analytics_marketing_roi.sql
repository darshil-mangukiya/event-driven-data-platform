explain (analyze false, buffers false, costs true)
select coalesce(marketing_campaign_id, 'unattributed') as marketing_campaign_id,
       count(*) as orders,
       round(sum(net_revenue), 2) as attributed_revenue,
       round(count(*) * 3.50, 2) as modeled_spend,
       round((sum(net_revenue) - count(*) * 3.50) / nullif(count(*) * 3.50, 0), 4) as roi
from processed_orders
where tenant_id = 'tenant_demo'
  and event_timestamp::date >= current_date - interval '30 day'
group by coalesce(marketing_campaign_id, 'unattributed')
order by attributed_revenue desc
limit 25 offset 0;

