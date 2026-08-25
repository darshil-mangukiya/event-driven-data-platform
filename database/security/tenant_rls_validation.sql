-- Manual validation query for tenant RLS.
-- Apply database/security/tenant_rls.sql first in a non-production sandbox.

begin;

select set_config('app.tenant_id', 'tenant_demo', true);

-- With RLS enforced, these result sets should only include tenant_demo rows.
select tenant_id, count(*) as rows_visible
from tenant_metrics_daily
group by tenant_id
order by tenant_id;

select tenant_id, count(*) as rows_visible
from raw_events
group by tenant_id
order by tenant_id;

rollback;
