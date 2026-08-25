select
    tenant_id,
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    gross_revenue,
    net_revenue,
    currency,
    status,
    channel,
    marketing_campaign_id,
    region,
    event_timestamp,
    event_timestamp::date as order_date
from {{ source('platform', 'processed_orders') }}

