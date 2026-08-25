explain (analyze false, buffers false, costs true)
select o.product_id,
       coalesce(p.name, o.product_id) as product_name,
       coalesce(p.category, 'unknown') as category,
       count(*) as orders,
       sum(o.quantity) as units_sold,
       round(sum(o.net_revenue), 2) as net_revenue,
       round(sum(o.net_revenue) / nullif(sum(o.quantity), 0), 2) as revenue_per_unit
from processed_orders o
left join tenant_products p
  on p.tenant_id = o.tenant_id
 and p.product_id = o.product_id
where o.tenant_id = 'tenant_demo'
  and o.event_timestamp::date >= current_date - interval '30 day'
group by o.product_id, coalesce(p.name, o.product_id), coalesce(p.category, 'unknown')
order by net_revenue desc
limit 25 offset 0;

