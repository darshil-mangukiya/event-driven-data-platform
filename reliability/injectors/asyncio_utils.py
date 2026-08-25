"""Run an async coroutine from scenario code that might be called either
standalone (no event loop running — e.g. a plain script) or from inside
`platform_cli`, which drives its whole command dispatch through
``asyncio.run()`` at the top level.

``asyncio.run()`` cannot be nested inside an already-running loop — this
was discovered for real by running ``platform_cli reliability run --all``
end-to-end (the redis-outage scenario raised
``RuntimeError: asyncio.run() cannot be called from a running event loop``
the first time every scenario ran back-to-back through the CLI, even though
it passed fine in isolation under pytest, which doesn't run its own loop).
"""

from __future__ import annotations

import asyncio
import concurrent.futures
from collections.abc import Coroutine
from typing import TypeVar

T = TypeVar("T")


def run_coroutine(coro: Coroutine[object, object, T]) -> T:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # No loop running in this thread — the common case (plain script, pytest).
        return asyncio.run(coro)
    else:
        # Already inside a running loop (e.g. platform_cli's own asyncio.run
        # around command dispatch) — asyncio.run() would raise if called
        # directly, so run the coroutine on its own loop in a separate
        # thread instead of nesting event loops.
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result()
