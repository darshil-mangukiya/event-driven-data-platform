select
    o.tenant_id,
    o.product_id,
    coalesce(p.name, o.product_id) as product_name,
    coalesce(p.category, 'unknown') as category,
    o.order_date,
    count(*) as order_count,
    sum(o.quantity) as units_sold,
    round(sum(o.net_revenue), 2) as net_revenue,
    round(sum(o.net_revenue) / nullif(sum(o.quantity), 0), 2) as revenue_per_unit
from {{ ref('stg_orders') }} o
left join {{ source('platform', 'tenant_products') }} p
    on p.tenant_id = o.tenant_id
    and p.product_id = o.product_id
group by 1, 2, 3, 4, 5

