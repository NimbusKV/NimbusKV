# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""persistence.py - NimbusKV v3 Phase 3.

Persistence is deliberately NOT where reads/writes happen -- ReactiveStore
(in-memory HAMT) is always the live, hot-path source of truth. A
PersistenceBackend is a write-through mirror + startup rehydration source,
nothing more. This keeps the lock-free read guarantees of
Phase 1 completely intact regardless of which backend is attached: readers
never touch SQLite or Redis, ever.

Each backend stores (key, pickled value, absolute expire_at timestamp or
None). load_all() is responsible for filtering out anything already expired
so NimbusKV never rehydrates dead keys.
"""
import pickle
import sqlite3
import threading
import time
from typing import Any, Iterable, List, Optional, Tuple


class PersistenceBackend:
    """Abstract interface for write-through persistence + startup
    rehydration.

    A backend is *never* on the hot read/write path -- ``ReactiveStore``
    is always the live source of truth. A backend only exists to (a)
    mirror writes somewhere durable, and (b) reload that durable state
    into the store once, at startup. Reads always come from memory.

    All methods here are synchronous/blocking. :class:`nimbuskv.NimbusKV`'s
    async methods (``aset``, ``adelete``) delegate these specific calls
    to a thread-pool executor when a real backend is attached -- that's
    a legitimate use of an executor because this is genuine I/O, unlike
    the in-memory read/write path which never needs one.

    To write a custom backend (e.g. a different database), subclass
    this and implement all four methods:

    Example:
        >>> class MyBackend(PersistenceBackend):
        ...     def load_all(self):
        ...         return []  # nothing to rehydrate
        ...     def persist_set(self, key, value, expire_at):
        ...         ...  # write to your store
        ...     def persist_delete(self, key):
        ...         ...  # remove from your store
        >>> from nimbuskv import NimbusKV
        >>> ld = NimbusKV(backend=MyBackend())
    """

    def load_all(self) -> List[Tuple[str, Any, Optional[float]]]:
        """Return every non-expired ``(key, value, expire_at_or_None)``
        currently in the backend. Called exactly once, at
        ``NimbusKV.__init__``, to rehydrate the in-memory store before
        the TTL scheduler starts. Implementations must filter out
        already-expired entries themselves -- NimbusKV trusts what
        this returns."""
        raise NotImplementedError

    def persist_set(self, key: str, value: Any, expire_at: Optional[float]) -> None:
        """Write-through mirror of a ``set()``. Called after the
        in-memory write has already succeeded -- this is durability,
        not the source of truth.

        Args:
            key: The key that was set.
            value: The value that was set.
            expire_at: Absolute Unix timestamp the key should expire
                at, or ``None`` for no expiry.
        """
        raise NotImplementedError

    def persist_delete(self, key: str) -> None:
        """Write-through mirror of a ``delete()`` (manual or TTL
        expiry). Called after the in-memory delete has already
        succeeded."""
        raise NotImplementedError

    def close(self) -> None:
        """Release any resources (connections, file handles). Called
        by ``NimbusKV.stop()``. Default implementation does nothing --
        override if your backend holds a resource that needs closing."""
        pass


class NullBackend(PersistenceBackend):
    """Pure in-memory mode: no persistence at all. This is the default
    backend (``NimbusKV()`` with no ``backend=`` argument) -- nothing
    survives a process restart, and ``NimbusKV``'s async methods stay
    genuinely non-blocking since there's no real I/O to delegate."""

    def load_all(self):
        return []

    def persist_set(self, key, value, expire_at):
        pass

    def persist_delete(self, key):
        pass


class SQLiteBackend(PersistenceBackend):
    """File-backed persistence via ``sqlite3``.

    One connection, guarded by a lock -- ``sqlite3`` connections aren't
    safe to share across threads without one. This lock only affects
    the persistence write-through path, not the in-memory reactive
    core, so it does not compromise the lock-free read/write
    guarantees of :class:`~.reactive_core.ReactiveStore`.

    Example:
        >>> from nimbuskv import NimbusKV
        >>> ld = NimbusKV(backend="sqlite:/path/to/store.db")
        >>> # or, to reuse an already-constructed backend instance:
        >>> backend = SQLiteBackend(path="/path/to/store.db")
        >>> ld = NimbusKV(backend=backend)
    """

    def __init__(self, path: str = "nimbuskv.db"):
        """
        Args:
            path: Filesystem path to the SQLite database file. Created
                if it doesn't exist.
        """
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS nimbuskv_kv ("
            "key TEXT PRIMARY KEY, value BLOB NOT NULL, expire_at REAL)"
        )
        self._conn.commit()

    def load_all(self):
        now = time.time()
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, value, expire_at FROM nimbuskv_kv"
            ).fetchall()
        out = []
        expired_keys = []
        for key, blob, expire_at in rows:
            if expire_at is not None and expire_at <= now:
                expired_keys.append(key)
                continue
            out.append((key, pickle.loads(blob), expire_at))
        if expired_keys:
            with self._lock:
                self._conn.executemany(
                    "DELETE FROM nimbuskv_kv WHERE key = ?",
                    [(k,) for k in expired_keys],
                )
                self._conn.commit()
        return out

    def persist_set(self, key, value, expire_at):
        blob = pickle.dumps(value)
        with self._lock:
            self._conn.execute(
                "INSERT INTO nimbuskv_kv (key, value, expire_at) VALUES (?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, expire_at=excluded.expire_at",
                (key, blob, expire_at),
            )
            self._conn.commit()

    def persist_delete(self, key):
        with self._lock:
            self._conn.execute("DELETE FROM nimbuskv_kv WHERE key = ?", (key,))
            self._conn.commit()

    def close(self):
        with self._lock:
            self._conn.close()


class RedisBackend(PersistenceBackend):
    """Redis-backed persistence, using Redis's own native TTL (``EX``)
    instead of reimplementing expiry -- Redis already expires keys
    itself, so :meth:`load_all` only needs to read what Redis still
    has and ask it for the remaining TTL (``PTTL``), rather than
    tracking expiry timestamps independently.

    Requires the optional ``redis`` dependency: ``pip install
    "nimbuskv[redis]"``.

    Example:
        >>> from nimbuskv import NimbusKV
        >>> ld = NimbusKV(backend="redis://localhost:6379/0")
        >>> # or with a pre-built client (e.g. for testing with
        >>> # fakeredis, or a custom connection pool):
        >>> import redis
        >>> backend = RedisBackend(client=redis.Redis())
        >>> ld = NimbusKV(backend=backend)
    """

    def __init__(self, url: Optional[str] = None, client: Optional[object] = None, namespace: str = "nimbuskv:"):
        """
        Args:
            url: A ``redis://`` or ``rediss://`` connection URL. Ignored
                if ``client`` is given.
            client: An already-constructed Redis client (e.g.
                ``redis.Redis()`` or a ``fakeredis`` instance for
                testing). Takes priority over ``url``.
            namespace: Key prefix used in Redis, so NimbusKV's keys
                don't collide with other data in the same Redis
                instance. Change this if you're sharing one Redis
                database across multiple NimbusKV instances/apps.
        """
        import redis as redis_module

        self._redis_module = redis_module
        self._ns = namespace
        if client is not None:
            self._client = client
        elif url is not None:
            self._client = redis_module.Redis.from_url(url)
        else:
            self._client = redis_module.Redis()

    def _k(self, key: str) -> str:
        return self._ns + key

    def load_all(self):
        out = []
        for raw_key in self._client.scan_iter(match=self._k("*")):
            key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
            key = key[len(self._ns):]
            blob = self._client.get(self._k(key))
            if blob is None:
                continue  # expired between SCAN and GET -- race is fine, just skip
            pttl = self._client.pttl(self._k(key))
            expire_at = time.time() + pttl / 1000.0 if pttl and pttl > 0 else None
            out.append((key, pickle.loads(blob), expire_at))
        return out

    def persist_set(self, key, value, expire_at):
        blob = pickle.dumps(value)
        if expire_at is not None:
            ttl_ms = max(1, int((expire_at - time.time()) * 1000))
            self._client.set(self._k(key), blob, px=ttl_ms)
        else:
            self._client.set(self._k(key), blob)

    def persist_delete(self, key):
        self._client.delete(self._k(key))

    def close(self):
        try:
            self._client.close()
        except Exception:
            pass


def resolve_backend(backend) -> PersistenceBackend:
    """Turn a ``backend=`` argument into a :class:`PersistenceBackend`
    instance. This is what :class:`nimbuskv.NimbusKV` calls internally
    -- you don't normally call this yourself.

    Args:
        backend: One of:

            - ``"memory"`` or ``None`` -- :class:`NullBackend` (default).
            - ``"sqlite"`` -- :class:`SQLiteBackend` at the default path
              (``nimbuskv.db``).
            - ``"sqlite:/path/to/file.db"`` -- :class:`SQLiteBackend`
              at the given path.
            - ``"redis"`` -- :class:`RedisBackend` at the default
              connection (``localhost:6379``).
            - ``"redis://host:port/db"`` -- :class:`RedisBackend` at
              the given URL.
            - An already-constructed :class:`PersistenceBackend`
              instance -- returned as-is (lets you pass a pre-configured
              backend, e.g. with a custom namespace or client).

    Returns:
        A :class:`PersistenceBackend` instance.

    Raises:
        ValueError: If ``backend`` doesn't match any recognized form.
    """
    if isinstance(backend, PersistenceBackend):
        return backend
    if backend is None:
        return NullBackend()
    if not isinstance(backend, str):
        raise ValueError(f"Unsupported backend spec: {backend!r}")
    if backend == "memory":
        return NullBackend()
    if backend == "sqlite":
        return SQLiteBackend()
    if backend.startswith("sqlite:"):
        return SQLiteBackend(path=backend.split(":", 1)[1])
    if backend == "redis":
        return RedisBackend()
    if backend.startswith("redis://") or backend.startswith("rediss://"):
        return RedisBackend(url=backend)
    raise ValueError(f"Unsupported backend spec: {backend!r}")
