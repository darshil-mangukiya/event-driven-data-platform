-- Mark a failed publish attempt or return it to pending with a retry delay.
-- Bindings: $1 outbox_id, $2 max_attempts, $3 retry_delay_seconds, $4 error_message.

update event_outbox
set status = case when attempts >= $2 then 'failed' else 'pending' end,
    available_at = now() + ($3::int * interval '1 second'),
    error_message = $4,
    updated_at = now()
where outbox_id = $1;
