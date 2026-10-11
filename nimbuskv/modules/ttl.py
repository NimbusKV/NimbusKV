# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""ttl.py - NimbusKV v3 TTL / expiry.

TTL is deliberately a thin layer on top of :class:`~.reactive_core.ReactiveStore`
rather than baked into the core -- ``ReactiveStore`` knows nothing about
time; this module just schedules a
``store.delete(key, _event="expire")`` call at the right moment.

If you're using :class:`nimbuskv.NimbusKV`, you never touch this module
directly -- ``ttl=`` on ``set()`` handles it for you. Read this if you're
building something custom on :class:`~.reactive_core.ReactiveStore` and
want expiry too.
"""
import heapq
import itertools
import logging
import threading
import time
from typing import Dict, List, Optional, Tuple

from .reactive_core import ReactiveStore

_log = logging.getLogger("nimbuskv")


class ExpiryScheduler(threading.Thread):
    """Background thread that deletes keys at their TTL deadline.

    Uses a min-heap of ``(when, key, generation)`` plus a condition
    variable, so the thread sleeps until the next actual expiry instead
    of polling in a loop.

    Generations exist to solve a real bug class: if a key is
    rescheduled (e.g. ``set(k, v, ttl=10)`` followed later by
    ``set(k, v2, ttl=3)``), there would otherwise be two heap entries
    for the same key. Each :meth:`schedule` call takes a fresh,
    process-unique generation number; :meth:`cancel` forgets the key.
    A popped heap entry only expires its key if its generation is still
    the live one -- stale entries are skipped.

    3.1.0 changes: generation numbers are unique across all keys (not a
    per-key counter), so a key with no pending TTL has no entry at all.
    v3.0.1 kept one entry for every key ever written, forever. The
    "is this still live?" check now also runs inside the store's write
    lock, so an expiry can never delete a value that was overwritten
    after the check.

    3.1.1 changes: the heap itself is also compacted periodically now
    (see :meth:`_compact`). Rescheduling a key's TTL leaves its old
    heap entry in place until that stale deadline is popped, so a hot
    key with a long TTL that is rewritten often would otherwise grow
    the heap without bound in the meantime -- the 3.1.0 fix above only
    bounded ``_generation``, not the heap.

    Example:
        >>> store = ReactiveStore()
        >>> scheduler = ExpiryScheduler(store)
        >>> scheduler.start()
        >>> store.set("temp", "value")
        >>> scheduler.schedule("temp", time.time() + 5)  # expires in 5s
    """

    def __init__(self, store: ReactiveStore):
        """
        Args:
            store: The :class:`~.reactive_core.ReactiveStore` this
                scheduler will call ``.delete(key, _event="expire")``
                on when a key's TTL elapses.
        """
        super().__init__(daemon=True, name="NimbusKV-ExpiryScheduler")
        self._store = store
        self._heap: List[Tuple[float, str, int]] = []
        # key -> (generation, expire_at) for keys with a pending TTL only.
        self._generation: Dict[str, Tuple[int, float]] = {}
        self._gen_counter = itertools.count(1)
        self._cv = threading.Condition()
        self._stop = False

    def schedule(self, key: str, when: float) -> None:
        """Schedule ``key`` to expire at absolute time ``when`` (as
        returned by ``time.time()``).

        Calling this again for the same key before it expires replaces
        the previous schedule -- the old deadline is invalidated via
        the generation counter, not left to fire alongside the new one.

        Args:
            key: The key to expire.
            when: Absolute Unix timestamp (``time.time() + ttl_seconds``)
                at which the key should be deleted.
        """
        with self._cv:
            gen = next(self._gen_counter)
            self._generation[key] = (gen, when)
            heapq.heappush(self._heap, (when, key, gen))
            if len(self._heap) > 2 * len(self._generation) + 64:
                self._compact()
            self._cv.notify()

    def _compact(self) -> None:
        """Rebuild the heap from live entries only, dropping stale
        entries left behind by reschedule/cancel.

        ``schedule()`` always pushes a new heap entry but never removes
        the old one for the same key, so repeatedly rescheduling a hot
        key's TTL (``set(k, v, ttl=...)`` over and over) grows the heap
        without bound until each stale entry's original deadline
        arrives and :meth:`run` pops and discards it. For a key with a
        long TTL that gets rewritten often, that's effectively
        unbounded growth in the meantime. Called from :meth:`schedule`
        once the heap is more than roughly 2x the number of live
        entries; caller holds ``_cv``.
        """
        live = self._generation
        self._heap = [
            entry for entry in self._heap
            if (live_entry := live.get(entry[1])) is not None
            and live_entry[0] == entry[2]
        ]
        heapq.heapify(self._heap)

    def cancel(self, key: str) -> None:
        """Cancel any pending expiry for ``key`` (e.g. it was
        overwritten with no TTL, or deleted manually before its TTL
        elapsed). Safe to call even if nothing was scheduled.

        Keys that never had a TTL return immediately without taking the
        scheduler lock -- this is called on every plain ``set()``.
        No wake-up is needed: the stale heap entry is skipped when it
        comes up."""
        if key not in self._generation:
            return
        with self._cv:
            self._generation.pop(key, None)

    def expire_at(self, key: str) -> Optional[float]:
        """Return the absolute expiry time pending for ``key``, or
        ``None`` if it has no TTL."""
        entry = self._generation.get(key)
        return entry[1] if entry is not None else None

    def run(self) -> None:
        """Scheduler main loop. Not called directly -- started via
        :meth:`threading.Thread.start` (NimbusKV does this for you)."""
        while True:
            with self._cv:
                if self._stop:
                    return
                if not self._heap:
                    self._cv.wait()
                    continue
                when, key, gen = self._heap[0]
                wait = when - time.time()
                if wait > 0:
                    self._cv.wait(timeout=wait)
                    continue
                heapq.heappop(self._heap)
                entry = self._generation.get(key)
                is_current = entry is not None and entry[0] == gen
            if not is_current:
                continue  # stale entry -- superseded by a later schedule/cancel

            def still_live(key=key, gen=gen) -> bool:
                # Runs under the store's write lock: a set()/cancel()
                # that happened after the check above wins.
                entry = self._generation.get(key)
                return entry is not None and entry[0] == gen

            try:
                self._store.delete(key, _event="expire", _only_if=still_live)
            except Exception:
                # Expiry firing must never kill the scheduler thread --
                # an error here (e.g. from a subscriber callback raising
                # inside delete()'s notify) would otherwise silently
                # stop every other key from ever expiring again.
                _log.exception("error while expiring key=%r", key)
            with self._cv:
                entry = self._generation.get(key)
                if entry is not None and entry[0] == gen:
                    del self._generation[key]

    def stop(self) -> None:
        """Stop the background thread. Called automatically by
        ``NimbusKV.stop()`` -- you don't normally need to call this
        yourself."""
        with self._cv:
            self._stop = True
            self._cv.notify()