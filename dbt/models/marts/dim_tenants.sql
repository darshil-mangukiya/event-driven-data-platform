select
    tenant_id,
    tenant_name,
    plan,
    region,
    is_active,
    config,
    created_at,
    updated_at
from {{ source('platform', 'tenant_config') }}

