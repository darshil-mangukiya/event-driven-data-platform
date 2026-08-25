select
    tenant_id,
    payment_id,
    order_id,
    customer_id,
    amount,
    currency,
    status,
    payment_method,
    failure_code,
    risk_score,
    event_timestamp,
    event_timestamp::date as payment_date
from {{ source('platform', 'processed_payments') }}

