-- Lease pending outbox events without blocking other dispatchers.
-- Bindings: $1 batch_size.

with next_batch as (
    select outbox_id
    from event_outbox
    where status = 'pending'
      and available_at <= now()
    order by created_at
    limit $1
    for update skip locked
)
update event_outbox o
set status = 'publishing',
    locked_at = now(),
    attempts = attempts + 1,
    updated_at = now()
from next_batch b
where o.outbox_id = b.outbox_id
returning o.*;
