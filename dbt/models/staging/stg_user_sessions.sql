select
    tenant_id,
    user_id,
    session_id,
    action,
    page,
    referrer,
    duration_seconds,
    plan,
    marketing_campaign_id,
    event_timestamp,
    event_timestamp::date as activity_date
from {{ source('platform', 'processed_user_sessions') }}

