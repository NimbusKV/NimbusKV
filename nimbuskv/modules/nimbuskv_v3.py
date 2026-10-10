# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""nimbuskv_v3.py - NimbusKV v3 public API.

This is the module you actually use: ``from nimbuskv import NimbusKV``.

Architecture, in one paragraph: a :class:`~.reactive_core.ReactiveStore`
(in-memory, immutable-snapshot core -- reads never block, each write
takes one short lock) is always the
live source of truth. An :class:`~.ttl.ExpiryScheduler` fires TTL expiry
against it. An optional :class:`~.persistence.PersistenceBackend`
(Memory/SQLite/Redis) mirrors writes through for durability and
rehydrates on startup -- but never sits on the read path.

Runs correctly on standard GIL Python and on free-threaded Python
(3.13t/3.14t) from the exact same code -- the concurrency strategy adapts
automatically (see ``nimbuskv.LOCK_FREE_MODE``), you don't need to do
anything differently for either.

Async honesty: for the default in-memory backend, ``aset``/``aget``/etc.
are genuinely non-blocking -- no thread spawned, because there's no real
I/O to hide behind an executor. When a real backend (SQLite/Redis) is
attached, *only* the persistence write is delegated to a thread-pool
executor -- the in-memory part of every operation still never blocks.

Sandbox (isolating callback execution) is intentionally not included in
this build. It's planned for a future release with real isolation
(subprocess/WASM-based), rather than the timeout-only wrapper v2 shipped
under that name.
"""
import asyncio
import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from .reactive_core import ReactiveStore, Subscription
from .ttl import ExpiryScheduler
from .persistence import PersistenceBackend, NullBackend, resolve_backend

_log = logging.getLogger("nimbuskv")
_MISSING = object()


class NimbusKV:
    """Reactive, TTL-aware, concurrent key/value store.

    The main class you'll use. Behaves like a thread-safe dict with
    expiry and a subscription/notification system layered on top, and
    an optional durable backend.

    Works correctly on standard GIL Python and free-threaded Python
    (3.13t/3.14t) from the same code path -- concurrency strategy adapts
    automatically underneath (see :data:`nimbuskv.LOCK_FREE_MODE`); you
    don't pass any flag for this.

    Can be used as a context manager to guarantee cleanup
    (``scheduler.stop()`` and ``backend.close()``) even if an exception
    is raised:

    Example:
        >>> from nimbuskv import NimbusKV
        >>> with NimbusKV() as ld:
        ...     ld.set("foo", 123)
        ...     print(ld.get("foo"))
        123
        >>> # scheduler + backend are cleanly stopped here automatically
    """

    def __init__(self, backend="memory"):
        """
        Args:
            backend: Where to persist data, beyond the always-live
                in-memory store. One of:

                - ``"memory"`` (default) -- no persistence, nothing
                  survives a restart.
                - ``"sqlite:/path/to/file.db"`` -- SQLite-backed.
                - ``"redis://host:port/db"`` -- Redis-backed (requires
                  ``pip install "nimbuskv[redis]"``).
                - A :class:`~.persistence.PersistenceBackend` instance,
                  for custom backends or pre-configured clients.

                If a backend with existing data is given, that data is
                loaded into memory immediately (already-expired keys
                are skipped) before this constructor returns.

        Example:
            >>> ld = NimbusKV()                              # in-memory
            >>> ld = NimbusKV(backend="sqlite:store.db")      # durable
            >>> ld = NimbusKV(backend="redis://localhost:6379/0")
        """
        self._store = ReactiveStore()
        self._scheduler = ExpiryScheduler(self._store)
        self._backend: PersistenceBackend = resolve_backend(backend)
        self._is_memory_only = isinstance(self._backend, NullBackend)
        self._stopped = False

        # Rehydrate from persistence before starting the scheduler, so
        # we don't race a background expiry against startup loading.
        for key, value, expire_at in self._backend.load_all():
            self._store.set(key, value)
            if expire_at is not None:
                self._scheduler.schedule(key, expire_at)

        self._scheduler.start()

    # ---- sync API --------------------------------------------------

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Set ``key`` to ``value``, optionally with a TTL.

        Args:
            key: The key to set.
            value: The value to store -- any Python object.
            ttl: Seconds until this key auto-expires. If omitted or
                ``None``, the key never expires (and any previously
                pending expiry for this key is cancelled).

        Example:
            >>> ld.set("foo", 123)
            >>> ld.set("session:abc", {"user": "alvin"}, ttl=3600)
        """
        expire_at = self._write(key, value, ttl)
        self._backend.persist_set(key, value, expire_at)

    def _write(self, key: str, value: Any, ttl: Optional[float]) -> Optional[float]:
        # Order matters: cancel any old TTL *before* the write, schedule
        # the new one *after* it. Then no expiry -- old or new -- can
        # ever fire against the value being written here.
        self._scheduler.cancel(key)
        self._store.set(key, value)
        if ttl is None:
            return None
        expire_at = time.time() + ttl
        self._scheduler.schedule(key, expire_at)
        return expire_at

    def mset(self, mapping: Dict[str, Any], ttl: Optional[float] = None) -> None:
        """Set multiple keys in a single atomic swap -- readers never
        observe a partial update (some keys changed, others not yet).

        Use this instead of a loop of individual :meth:`set` calls
        whenever the keys are logically related and must appear
        together, or when you're writing many keys at once and want
        fewer separate compare-and-swap attempts under contention.

        Args:
            mapping: ``{key: value}`` pairs to set together.
            ttl: If given, applied to *all* keys in ``mapping``
                (same expiry time for each). For per-key TTLs, call
                :meth:`set` individually instead.

        Example:
            >>> ld.mset({"account_a": 90, "account_b": 110})
        """
        for key in mapping:
            self._scheduler.cancel(key)
        self._store.mset(mapping)
        expire_at = time.time() + ttl if ttl is not None else None
        for key, value in mapping.items():
            if expire_at is not None:
                self._scheduler.schedule(key, expire_at)
            self._backend.persist_set(key, value, expire_at)

    def get(self, key: str, default: Any = None) -> Any:
        """Get the value for ``key``, or ``default`` if absent/expired.

        Always reads from the in-memory store -- never touches the
        persistence backend, so this is fast and non-blocking
        regardless of which backend is configured.

        Example:
            >>> ld.get("foo")
            123
            >>> ld.get("missing", "n/a")
            'n/a'
        """
        return self._store.get(key, default)

    def mget(self, keys, default: Any = None) -> Dict[str, Any]:
        """Get multiple keys at once.

        This is a plain convenience loop, not a special atomic
        operation -- reads never block or contend with writers
        individually, so there's no correctness reason to batch them;
        it's just less typing than calling :meth:`get` in a loop
        yourself.

        Args:
            keys: Iterable of keys to fetch.
            default: Value used for any key that's absent or expired.

        Returns:
            ``{key: value_or_default}`` for every key in ``keys``.

        Example:
            >>> ld.mset({"a": 1, "b": 2})
            >>> ld.mget(["a", "b", "missing"])
            {'a': 1, 'b': 2, 'missing': None}
        """
        return {k: self._store.get(k, default) for k in keys}

    def delete(self, key: str) -> bool:
        """Delete ``key`` if present. Also cancels any pending TTL for
        it, so a manual delete never races with a later automatic
        expiry.

        Returns:
            ``True`` if the key existed and was deleted, ``False`` if
            it was already absent.

        Example:
            >>> ld.set("foo", 1)
            >>> ld.delete("foo")
            True
        """
        self._scheduler.cancel(key)
        existed = self._store.delete(key)
        self._backend.persist_delete(key)
        return existed

    def exists(self, key: str) -> bool:
        """Return ``True`` if ``key`` is currently present (equivalent
        to ``key in ld``)."""
        return self._store.exists(key)

    def keys(self) -> List[str]:
        """Return a point-in-time snapshot list of all current keys."""
        return self._store.keys()

    def items(self) -> List[Tuple[str, Any]]:
        """Return a point-in-time snapshot list of all
        ``(key, value)`` pairs."""
        return self._store.items()

    def subscribe(
        self,
        fn: Callable[[str, str, Any, Any], None],
        key: Optional[str] = None,
    ) -> Subscription:
        """Subscribe to live changes on this store.

        Args:
            fn: Called as ``fn(key, event, old_value, new_value)`` on
                every successful change. ``event`` is one of
                ``"set"``, ``"delete"``, or ``"expire"`` (TTL expiry
                fires ``"expire"``, distinct from a manual
                :meth:`delete`, so you can tell them apart). Runs
                synchronously in the thread that made the change --
                keep it fast, since a slow subscriber delays that
                caller's ``set``/``delete``/etc. from returning.
            key: Subscribe to just this key, or ``None`` for every key.

        Returns:
            A :class:`~.reactive_core.Subscription` -- call
            ``.cancel()`` to stop.

        Example:
            >>> def log_change(key, event, old, new):
            ...     print(f"{key} {event}: {old!r} -> {new!r}")
            >>> sub = ld.subscribe(log_change)
            >>> ld.set("foo", 1)
            foo set: None -> 1
            >>> sub.cancel()
        """
        return self._store.subscribe(fn, key=key)

    def atomic(self, key: str, fn: Callable[[Any], Any], default: Any = None) -> Any:
        """Atomic compound read-modify-write on a single key -- e.g. an
        increment that's safe under concurrent writers.

        ``fn`` runs exactly once, while the store's write lock is held.
        Readers are never blocked by it, but other writers wait, so keep
        it short. It must not write to this same store (that raises
        :class:`~.reactive_core.ReentrantWriteError` instead of
        deadlocking). Any TTL already on ``key`` is kept.

        Args:
            key: The key to read, transform, and write back.
            fn: A function ``(current_value) -> new_value``.
            default: Value passed to ``fn`` if ``key`` doesn't exist
                yet.

        Returns:
            The new value that was written.

        Example:
            >>> ld.set("counter", 0)
            >>> ld.atomic("counter", lambda v: v + 1)
            1
            >>> # Safe even with many threads calling this concurrently:
            >>> # no lost updates, no lock held during the increment.
        """
        new_value = self._store.atomic(key, fn, default=default)
        # Persist the key's real pending expiry. v3.0.1 wrote None here,
        # so a TTL key touched by atomic() never expired after a restart.
        self._backend.persist_set(key, new_value, self._scheduler.expire_at(key))
        return new_value

    def wait_for(
        self,
        key: str,
        predicate: Callable[[Any], bool],
        timeout: Optional[float] = None,
    ) -> Any:
        """Block the calling thread until ``predicate(current_value)``
        is true, using the reactive subscription system instead of
        polling.

        This is the pattern NimbusKV's reactivity is *for*: coordinate
        threads/producers-consumers around shared state without a
        sleep-and-poll loop burning CPU, and without hand-rolling your
        own ``threading.Event`` plumbing every time.

        Args:
            key: The key to watch.
            predicate: Called with the key's current value (and again
                on every subsequent change) -- return ``True`` when
                the condition you're waiting for is met.
            timeout: Maximum seconds to wait. ``None`` (default) waits
                forever.

        Returns:
            The value that satisfied ``predicate``.

        Raises:
            TimeoutError: If ``timeout`` elapses before ``predicate``
                is satisfied.

        Example:
            >>> # Thread A:
            >>> ld.wait_for("job:status", lambda v: v == "done", timeout=30)
            >>> # Thread B, elsewhere, eventually:
            >>> ld.set("job:status", "done")
            >>> # Thread A's wait_for() returns "done" as soon as that
            >>> # set() happens -- no polling in between.
        """
        current = self._store.get(key)
        if predicate(current):
            return current

        event = threading.Event()
        result: Dict[str, Any] = {}

        def _check(k, ev, old, new):
            if predicate(new):
                result["value"] = new
                event.set()

        sub = self._store.subscribe(_check, key=key)
        try:
            # Re-check after subscribing: closes the race where the
            # value already satisfied `predicate` in the gap between
            # our first check above and the subscription being
            # registered.
            current = self._store.get(key)
            if predicate(current):
                return current
            if event.wait(timeout):
                return result["value"]
            raise TimeoutError(
                f"wait_for(key={key!r}) timed out after {timeout}s"
            )
        finally:
            sub.cancel()

    def stop(self) -> None:
        """Stop the background TTL scheduler thread and close the
        persistence backend (if any). Safe to call more than once.

        Not calling this eventually happens anyway via ``__del__``, but
        explicit ``stop()`` (or using ``NimbusKV`` as a context
        manager) is recommended for prompt, deterministic cleanup --
        especially for the SQLite backend, where you want the
        connection closed at a known point rather than whenever
        garbage collection happens to run.
        """
        if not self._stopped:
            self._scheduler.stop()
            self._backend.close()
            self._stopped = True

    # ---- async API --------------------------------------------------
    # Genuinely async for the in-memory path: no run_in_executor, no
    # thread spawned per call, because the underlying operation is a
    # non-blocking snapshot read / O(log32 n) persistent-map update,
    # not real I/O. Only the persistence write (when a real backend is
    # attached) is delegated to an executor, because that IS real I/O.

    async def aset(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Async version of :meth:`set`. Genuinely non-blocking for the
        default in-memory backend; delegates only the persistence
        write to a thread-pool executor when a real backend (SQLite/
        Redis) is attached.

        Example:
            >>> await ld.aset("foo", 123)
        """
        if self._is_memory_only:
            self.set(key, value, ttl=ttl)
            return
        expire_at = self._write(key, value, ttl)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._backend.persist_set, key, value, expire_at)

    async def aget(self, key: str, default: Any = None) -> Any:
        """Async version of :meth:`get`. Always reads from memory --
        never touches the backend -- so this is always truly
        non-blocking regardless of which backend is attached.

        Example:
            >>> await ld.aget("foo")
            123
        """
        return self._store.get(key, default)

    async def adelete(self, key: str) -> bool:
        """Async version of :meth:`delete`.

        Example:
            >>> await ld.adelete("foo")
            True
        """
        self._scheduler.cancel(key)
        existed = self._store.delete(key)
        if self._is_memory_only:
            return existed
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._backend.persist_delete, key)
        return existed

    async def aexists(self, key: str) -> bool:
        """Async version of :meth:`exists`."""
        return self._store.exists(key)

    # ---- dunder convenience -------------------------------------------

    def __contains__(self, key: str) -> bool:
        return self._store.exists(key)

    def __len__(self) -> int:
        return len(self._store)

    def __iter__(self):
        for k in self._store.keys():
            yield k

    def __repr__(self) -> str:
        backend_name = type(self._backend).__name__
        return f"<NimbusKV size={len(self._store)} backend={backend_name}>"

    def __enter__(self) -> "NimbusKV":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    def __del__(self):
        try:
            self.stop()
        except Exception:
            pass
