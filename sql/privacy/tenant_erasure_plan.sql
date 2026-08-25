-- Tenant-scoped subject erasure plan.
-- Bindings: $1 tenant_id, $2 subject_id, $3 requested_by, $4 reason.
-- Run in a governed workflow after legal/business approval.

insert into privacy_erasure_requests (
    tenant_id, subject_id, subject_type, status, requested_by, reason, requested_at
)
values ($1, $2, 'user', 'running', $3, $4, now());

update processed_user_sessions
set user_id = concat('erased:', encode(digest($2, 'sha256'), 'hex')),
    page = null,
    referrer = null,
    marketing_campaign_id = null
where tenant_id = $1
  and user_id = $2;

update raw_events
set payload = payload - 'email' - 'ip_address' - 'user_agent'
where tenant_id = $1
  and payload::text like concat('%', $2, '%');

update privacy_erasure_requests
set status = 'completed',
    completed_at = now(),
    evidence = jsonb_build_object('subject_id_hash', encode(digest($2, 'sha256'), 'hex'))
where tenant_id = $1
  and subject_id = $2
  and status = 'running';
