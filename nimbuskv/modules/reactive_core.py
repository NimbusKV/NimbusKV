# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""reactive_core.py - NimbusKV v3 core engine.

This module implements the immutable-snapshot state container that
every other part of NimbusKV sits on top of. If you're just using
``NimbusKV`` (from ``nimbuskv import NimbusKV``), you don't need to import
anything from here directly -- this is the internals. Read this file if
you want to understand *why* NimbusKV is safe under concurrent access, or
if you're building something custom on top of ``ReactiveStore`` directly.

Concurrency model (since 3.1.0)
Readers never lock. CPython guarantees a single reference write is not
torn, so a reader of ``self._ref`` always sees either the old snapshot or
a fully-formed new one. That is unchanged from v3.

Writers take one short mutex, compute the new snapshot from the current
one, publish it, and release. v3.0.1 instead computed the new snapshot
*outside* the lock and retried on conflict ("optimistic CAS"). Under a
hot key that turned into a retry storm: every losing thread threw its
work away and immediately tried again, so throughput fell as threads were
added. A plain mutex never wastes work -- each acquisition does exactly
one useful write -- and measured flat-or-better at every thread count on
both GIL and free-threaded builds.

Side benefits of the mutex: the function passed to :meth:`mutate` /
:meth:`ReactiveStore.atomic` now runs exactly once per call (it used to
re-run on every lost race), and old values reported to subscribers are
always the ones the write actually replaced.
"""
import logging
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple

import immutables

from .runtime import LOCK_FREE_MODE
from .exceptions import NimbusKVError

_MISSING = object()
_log = logging.getLogger("nimbuskv")


class ConflictRetryExceeded(NimbusKVError):
    """Deprecated: never raised since 3.1.0 (writes no longer retry). Kept
    so ``except ConflictRetryExceeded`` in existing code still imports.

    Originally raised when a mutation could not win the compare-and-swap
    race within ``max_retries`` attempts.

    This should only happen under pathological write contention -- many
    threads hammering the *same* store with expensive mutations
    simultaneously. If you hit this in practice, consider batching writes
    with :meth:`ReactiveStore.mset` instead of many individual
    :meth:`ReactiveStore.set` calls, since batching reduces the number of
    separate swap attempts needed.
    """


class ReentrantWriteError(NimbusKVError):
    """Raised when a function passed to :meth:`ReactiveStore.mutate` or
    :meth:`ReactiveStore.atomic` tries to write to the same store.

    The write lock is held while that function runs, so a nested write
    from the same thread would deadlock. Raising is better than hanging.
    Do the second write after the first call returns, or combine both
    into one :meth:`ReactiveStore.mutate` / ``mset`` call.
    """


class Subscription:
    """Handle returned by :meth:`ReactiveStore.subscribe`.

    Call :meth:`cancel` when you no longer want to receive notifications.
    Forgetting to cancel a subscription just means the callback keeps
    firing -- it does not leak a thread or a lock, but it's good practice
    to cancel subscriptions you no longer need (e.g. in a request handler
    that subscribes temporarily and should clean up afterwards).

    Example:
        >>> sub = store.subscribe(lambda k, e, o, n: print(k, e))
        >>> store.set("x", 1)
        x set
        >>> sub.cancel()
        >>> store.set("x", 2)  # no output now
    """

    def __init__(self, store: "ReactiveStore", key: Optional[str], sub_id: int):
        self._store = store
        self._key = key
        self._id = sub_id
        self._cancelled = False

    def cancel(self) -> None:
        """Stop receiving notifications from this subscription. Safe to
        call more than once."""
        if not self._cancelled:
            self._store._unsubscribe(self._key, self._id)
            self._cancelled = True


class ReactiveStore:
    """Immutable-snapshot, concurrent key/value core.

    This is the engine underneath NimbusKV's public API -- it knows
    nothing about TTL, persistence backends, or the sync/async split.
    It only guarantees three things:

    1. Reads never block, ever, regardless of what writers are doing.
    2. Writes never lose an update, even under heavy concurrent
       contention -- each write is applied to the latest snapshot under
       a short write lock.
    3. Every successful write pushes a notification to subscribers.

    You will normally use this through :class:`nimbuskv.NimbusKV`
    rather than directly -- but it's a complete, independently usable
    concurrent store if you need lower-level control (e.g. building a
    custom persistence strategy).

    Example:
        >>> store = ReactiveStore()
        >>> store.set("foo", 1)
        >>> store.get("foo")
        1
        >>> store.mutate(lambda m: m.set("foo", m.get("foo", 0) + 1))
        >>> store.get("foo")
        2
    """

    def __init__(self, max_retries: int = 1000):
        """
        Args:
            max_retries: Deprecated and ignored since 3.1.0. Writes no
                longer retry, so :class:`ConflictRetryExceeded` is never
                raised. Kept so existing code keeps working.
        """
        self._ref: immutables.Map = immutables.Map()
        self._swap_lock = threading.Lock()
        self._writer: Optional[int] = None  # thread id holding _swap_lock
        # Kept only for API compatibility; mutate() no longer retries.
        self._max_retries = max_retries
        # Copy-on-write: subscribe/unsubscribe replace this whole dict
        # (under _subs_lock); _notify() reads it with no lock at all.
        self._subs: Dict[Optional[str], Tuple[Tuple[int, Callable], ...]] = {}
        self._subs_lock = threading.Lock()
        self._next_sub_id = 0

    # ---- reads: no lock, ever ----------------------------------------

    def snapshot(self) -> immutables.Map:
        """Return the current immutable snapshot.

        This is an O(1), lock-free read of the live state as a whole
        (rather than one key). The returned :class:`immutables.Map` is
        itself immutable, so it's always safe to iterate/inspect even
        while other threads keep writing to the store.

        Returns:
            The current ``immutables.Map`` snapshot.

        Example:
            >>> store.snapshot()
            immutables.Map({'foo': 2})
        """
        return self._ref

    def get(self, key: str, default: Any = _MISSING) -> Any:
        """Read a single key. Never blocks, never contends with writers.

        Args:
            key: The key to read.
            default: Value to return if ``key`` isn't present. If
                omitted, returns ``None`` for a missing key (matching
                ``dict.get``'s common usage, not raising ``KeyError``).

        Returns:
            The stored value, or ``default`` (or ``None``) if absent.

        Example:
            >>> store.get("missing")
            None
            >>> store.get("missing", "fallback")
            'fallback'
        """
        val = self._ref.get(key, _MISSING)
        if val is _MISSING:
            if default is _MISSING:
                return None
            return default
        return val

    def exists(self, key: str) -> bool:
        """Return ``True`` if ``key`` is currently present.

        Example:
            >>> store.set("a", 1)
            >>> store.exists("a")
            True
        """
        return key in self._ref

    def keys(self) -> List[str]:
        """Return a snapshot list of all current keys.

        This is a point-in-time copy -- it won't change under you even
        if writers are actively modifying the store while you iterate
        it, unlike iterating a plain ``dict`` under concurrent
        mutation.
        """
        return list(self._ref.keys())

    def items(self) -> List[Tuple[str, Any]]:
        """Return a snapshot list of all current ``(key, value)`` pairs.
        Same point-in-time-safe guarantee as :meth:`keys`."""
        return list(self._ref.items())

    def __len__(self) -> int:
        return len(self._ref)

    # ---- writes: one short mutex per write ------------------------------

    def _apply(self, fn: Callable[[immutables.Map], immutables.Map]) -> immutables.Map:
        """Run ``fn`` on the current snapshot under the write lock and
        publish its result. Every write path goes through here."""
        lock = self._swap_lock
        if not lock.acquire(blocking=False):
            if self._writer == threading.get_ident():
                raise ReentrantWriteError(
                    "cannot write to a NimbusKV store from inside a "
                    "mutate()/atomic() function on the same store"
                )
            lock.acquire()
        self._writer = threading.get_ident()
        try:
            old = self._ref
            new = fn(old)
            if new is not old:
                self._ref = new
            return new
        finally:
            self._writer = None
            lock.release()

    def mutate(self, fn: Callable[[immutables.Map], immutables.Map]) -> immutables.Map:
        """Apply an arbitrary transformation to the whole map, atomically.

        This is the primitive every other write operation (``set``,
        ``delete``, ``mset``) is built from. Use it directly when you
        need a transformation that :meth:`set`/:meth:`delete` don't
        cover -- e.g. "set this key only if it doesn't already exist",
        or any multi-key change that must be applied as a single atomic
        swap.

        Args:
            fn: A function ``(old_map) -> new_map``. It runs exactly
                once, while the store's write lock is held, so keep it
                short: readers are never blocked, but other writers wait
                for it. It must not write to this same store (that
                raises :class:`ReentrantWriteError`). If ``fn`` returns
                the same object it was given (``new is old``), nothing
                is swapped.

        Returns:
            The new snapshot (or the unchanged one, if ``fn`` was a
            no-op).

        Example:
            >>> # "set only if absent" -- not expressible with plain set()
            >>> store.mutate(
            ...     lambda m: m if "lock" in m else m.set("lock", "held")
            ... )
        """
        return self._apply(fn)

    def set(self, key: str, value: Any) -> None:
        """Set a single key, atomically.

        Args:
            key: The key to set.
            value: The value to store. Any Python object.

        Example:
            >>> store.set("foo", 123)
            >>> store.get("foo")
            123
        """
        box = [_MISSING]

        def _txn(m: immutables.Map) -> immutables.Map:
            box[0] = m.get(key, _MISSING)  # the value this write replaces
            return m.set(key, value)

        self._apply(_txn)
        old_val = box[0]
        self._notify(key, "set", None if old_val is _MISSING else old_val, value)

    def mset(self, mapping: Dict[str, Any]) -> immutables.Map:
        """Set multiple keys in a **single atomic swap**.

        This is not just a convenience wrapper around calling
        :meth:`set` in a loop -- it matters for correctness and
        performance under contention. Each individual ``set()`` call is
        its own write; if you have five related keys to update
        together, five separate ``set()`` calls take the write lock five
        times, and -- more importantly -- other readers could observe
        the update
        *partially applied* (key 1 through 3 changed, 4 and 5 not yet).
        ``mset`` applies all of them in one swap: readers see either
        the fully-old or the fully-new state, never a partial mix.

        Args:
            mapping: Dict of ``{key: value}`` pairs to set together.

        Returns:
            The new snapshot after the swap.

        Example:
            >>> # Move money between two accounts -- must be atomic,
            >>> # a reader must never see only one side updated.
            >>> store.mset({"account_a": 90, "account_b": 110})
        """
        old_vals: Dict[str, Any] = {}

        def _txn(m: immutables.Map) -> immutables.Map:
            with m.mutate() as mm:
                for k, v in mapping.items():
                    old_vals[k] = mm.get(k, _MISSING)
                    mm[k] = v
                return mm.finish()

        new = self._apply(_txn)
        for k, v in mapping.items():
            old = old_vals[k]
            self._notify(k, "set", None if old is _MISSING else old, v)
        return new

    def atomic(self, key: str, fn: Callable[[Any], Any], default: Any = None) -> Any:
        """Atomic compound read-modify-write on a single key, with a
        notification fired after the winning swap -- e.g. a counter
        increment that's safe under concurrent writers **and** visible
        to :meth:`subscribe` / anything built on it (such as
        :meth:`livedict.LiveDict.wait_for`... see
        :class:`livedict.LiveDict.atomic` for the public-facing
        version most users call).

        This exists at the ``ReactiveStore`` level (not just on
        :class:`livedict.LiveDict`) specifically so the notification is
        part of the same atomic operation as the swap -- computing the
        new value via :meth:`mutate` and firing the notification as a
        separate step would leave a window where a subscriber could
        read a value that hasn't been announced yet, or never gets
        announced at all if the calling code forgets to notify (this
        was a real bug: an earlier version of the higher-level
        ``atomic()`` called :meth:`mutate` directly and never notified
        subscribers at all, so :meth:`subscribe` and anything built on
        it -- like a blocking wait on a condition -- silently never
        fired for atomic updates).

        Args:
            key: The key to read, transform, and write back.
            fn: A function ``(current_value) -> new_value``. Runs
                exactly once, under the write lock -- keep it short and
                don't write to this store from inside it.
            default: Value passed to ``fn`` if ``key`` doesn't exist
                yet.

        Returns:
            The new value that was written.

        Example:
            >>> store.set("counter", 0)
            >>> store.atomic("counter", lambda v: v + 1)
            1
        """
        box: Dict[str, Any] = {}

        def _txn(m: immutables.Map) -> immutables.Map:
            old = m.get(key, _MISSING)
            box["old"] = old
            current = m.get(key, default)
            box["new_value"] = fn(current)
            return m.set(key, box["new_value"])

        self._apply(_txn)
        old = box["old"]
        self._notify(key, "set", None if old is _MISSING else old, box["new_value"])
        return box["new_value"]

    def delete(
        self,
        key: str,
        _event: str = "delete",
        _only_if: Optional[Callable[[], bool]] = None,
    ) -> bool:
        """Delete a key if present.

        Args:
            key: The key to delete.
            _event: Internal use only. TTL expiry reuses this exact
                method but passes ``_event="expire"`` so subscribers
                can tell an automatic expiry apart from a manual
                delete. Don't pass this yourself.
            _only_if: Internal use only. Checked under the write lock;
                the delete happens only if it returns ``True``. TTL
                expiry uses it so an expiry that has been superseded
                can never delete a newer value.

        Returns:
            ``True`` if the key existed and was removed, ``False`` if
            it was already absent (a no-op, nothing is swapped or
            notified in that case).

        Example:
            >>> store.set("foo", 1)
            >>> store.delete("foo")
            True
            >>> store.delete("foo")  # already gone
            False
        """
        if key not in self._ref:
            return False  # lock-free fast path; rechecked under the lock
        box = [_MISSING]

        def _txn(m: immutables.Map) -> immutables.Map:
            if key not in m or (_only_if is not None and not _only_if()):
                return m
            box[0] = m[key]
            return m.delete(key)

        self._apply(_txn)
        if box[0] is _MISSING:
            return False  # someone else removed it first: no notification
        self._notify(key, _event, box[0], None)
        return True

    # ---- reactive subscriptions ----------------------------------------

    def subscribe(
        self,
        fn: Callable[[str, str, Any, Any], None],
        key: Optional[str] = None,
    ) -> Subscription:
        """Subscribe to changes on this store.

        Args:
            fn: Called as ``fn(key, event, old_value, new_value)`` after
                every successful write that matches ``key``.
                ``event`` is one of ``"set"``, ``"delete"``, or
                ``"expire"``. Callbacks run synchronously, in the
                thread that performed the write, immediately after that
                write's swap succeeds -- so keep them fast; a slow
                callback delays whoever made the change (and, for
                :class:`nimbuskv.NimbusKV`, delays that ``set``/
                ``delete`` call from returning).
            key: If given, only notified about changes to this specific
                key. If ``None`` (default), notified about every key.

        Returns:
            A :class:`Subscription` handle -- call ``.cancel()`` on it
            to stop receiving notifications.

        Example:
            >>> def on_change(key, event, old, new):
            ...     print(f"{key}: {event} {old!r} -> {new!r}")
            >>> sub = store.subscribe(on_change, key="foo")
            >>> store.set("foo", 1)
            foo: set None -> 1
            >>> sub.cancel()
        """
        with self._subs_lock:
            sub_id = self._next_sub_id
            self._next_sub_id += 1
            subs = dict(self._subs)
            subs[key] = subs.get(key, ()) + ((sub_id, fn),)
            self._subs = subs
        return Subscription(self, key, sub_id)

    def _unsubscribe(self, key: Optional[str], sub_id: int) -> None:
        with self._subs_lock:
            lst = self._subs.get(key)
            if lst:
                subs = dict(self._subs)
                remaining = tuple((i, f) for i, f in lst if i != sub_id)
                if remaining:
                    subs[key] = remaining
                else:
                    del subs[key]
                self._subs = subs

    def _notify(self, key: str, event: str, old_value: Any, new_value: Any) -> None:
        # No lock: self._subs is replaced wholesale on every
        # subscribe/unsubscribe, so this one read is a consistent
        # snapshot. With no subscribers this returns after two dict
        # lookups (v3.0.1 took a lock and copied two lists every write).
        subs = self._subs
        if not subs:
            return
        targeted = subs.get(key, ())
        general = subs.get(None, ())
        if not targeted and not general:
            return
        for _, fn in targeted + general:
            try:
                fn(key, event, old_value, new_value)
            except Exception:
                # A misbehaving subscriber must never break the write
                # path for everyone else. Logged, not raised.
                _log.exception(
                    "subscriber callback raised for key=%r event=%r", key, event
                )
