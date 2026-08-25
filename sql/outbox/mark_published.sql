-- Mark a successfully published outbox row.
-- Bindings: $1 outbox_id.

update event_outbox
set status = 'published',
    published_at = now(),
    error_message = null,
    updated_at = now()
where outbox_id = $1;
