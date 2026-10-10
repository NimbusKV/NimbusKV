# NimbusKV API Reference

Generated from the library's own docstrings - every public class and method below is documented at the source; this file exists so you can browse the full surface in one place without opening five modules. Regenerate with `python dev/generate_api_reference.py` after any public API change, so this never drifts from the actual code.


## Package

`nimbuskv.__version__` = `3.1.0`


Exported names: ConflictRetryExceeded, LOCK_FREE_MODE, NimbusKV, NimbusKVError, NullBackend, PersistenceBackend, RedisBackend, ReentrantWriteError, SQLiteBackend, Subscription, gil_enabled, is_free_threaded_build


## `NimbusKV`

Reactive, TTL-aware, concurrent key/value store.

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


### `__init__(self, backend='memory')`

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


### `set(self, key: str, value: Any, ttl: Optional[float] = None) -> None`

Set ``key`` to ``value``, optionally with a TTL.

Args:
    key: The key to set.
    value: The value to store -- any Python object.
    ttl: Seconds until this key auto-expires. If omitted or
        ``None``, the key never expires (and any previously
        pending expiry for this key is cancelled).

Example:
    >>> ld.set("foo", 123)
    >>> ld.set("session:abc", {"user": "alvin"}, ttl=3600)


### `mset(self, mapping: Dict[str, Any], ttl: Optional[float] = None) -> None`

Set multiple keys in a single atomic swap -- readers never
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


### `get(self, key: str, default: Any = None) -> Any`

Get the value for ``key``, or ``default`` if absent/expired.

Always reads from the in-memory store -- never touches the
persistence backend, so this is fast and non-blocking
regardless of which backend is configured.

Example:
    >>> ld.get("foo")
    123
    >>> ld.get("missing", "n/a")
    'n/a'


### `mget(self, keys, default: Any = None) -> Dict[str, Any]`

Get multiple keys at once.

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


### `delete(self, key: str) -> bool`

Delete ``key`` if present. Also cancels any pending TTL for
it, so a manual delete never races with a later automatic
expiry.

Returns:
    ``True`` if the key existed and was deleted, ``False`` if
    it was already absent.

Example:
    >>> ld.set("foo", 1)
    >>> ld.delete("foo")
    True


### `exists(self, key: str) -> bool`

Return ``True`` if ``key`` is currently present (equivalent
to ``key in ld``).


### `keys(self) -> List[str]`

Return a point-in-time snapshot list of all current keys.


### `items(self) -> List[Tuple[str, Any]]`

Return a point-in-time snapshot list of all
``(key, value)`` pairs.


### `subscribe(self, fn: Callable[[str, str, Any, Any], NoneType], key: Optional[str] = None) -> nimbuskv.modules.reactive_core.Subscription`

Subscribe to live changes on this store.

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


### `atomic(self, key: str, fn: Callable[[Any], Any], default: Any = None) -> Any`

Atomic compound read-modify-write on a single key -- e.g. an
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


### `wait_for(self, key: str, predicate: Callable[[Any], bool], timeout: Optional[float] = None) -> Any`

Block the calling thread until ``predicate(current_value)``
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


### `stop(self) -> None`

Stop the background TTL scheduler thread and close the
persistence backend (if any). Safe to call more than once.

Not calling this eventually happens anyway via ``__del__``, but
explicit ``stop()`` (or using ``NimbusKV`` as a context
manager) is recommended for prompt, deterministic cleanup --
especially for the SQLite backend, where you want the
connection closed at a known point rather than whenever
garbage collection happens to run.


### `aset(self, key: str, value: Any, ttl: Optional[float] = None) -> None`

Async version of :meth:`set`. Genuinely non-blocking for the
default in-memory backend; delegates only the persistence
write to a thread-pool executor when a real backend (SQLite/
Redis) is attached.

Example:
    >>> await ld.aset("foo", 123)


### `aget(self, key: str, default: Any = None) -> Any`

Async version of :meth:`get`. Always reads from memory --
never touches the backend -- so this is always truly
non-blocking regardless of which backend is attached.

Example:
    >>> await ld.aget("foo")
    123


### `adelete(self, key: str) -> bool`

Async version of :meth:`delete`.

Example:
    >>> await ld.adelete("foo")
    True


### `aexists(self, key: str) -> bool`

Async version of :meth:`exists`.


## `ReactiveStore` (advanced - usually accessed via `NimbusKV`, not directly)

Immutable-snapshot, concurrent key/value core.

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


### `get(self, key: str, default: Any = <object object at 0x7f97e7580960>) -> Any`

Read a single key. Never blocks, never contends with writers.

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


### `exists(self, key: str) -> bool`

Return ``True`` if ``key`` is currently present.

Example:
    >>> store.set("a", 1)
    >>> store.exists("a")
    True


### `keys(self) -> List[str]`

Return a snapshot list of all current keys.

This is a point-in-time copy -- it won't change under you even
if writers are actively modifying the store while you iterate
it, unlike iterating a plain ``dict`` under concurrent
mutation.


### `items(self) -> List[Tuple[str, Any]]`

Return a snapshot list of all current ``(key, value)`` pairs.
Same point-in-time-safe guarantee as :meth:`keys`.


### `snapshot(self) -> immutables._map.Map`

Return the current immutable snapshot.

This is an O(1), lock-free read of the live state as a whole
(rather than one key). The returned :class:`immutables.Map` is
itself immutable, so it's always safe to iterate/inspect even
while other threads keep writing to the store.

Returns:
    The current ``immutables.Map`` snapshot.

Example:
    >>> store.snapshot()
    immutables.Map({'foo': 2})


### `mutate(self, fn: Callable[[immutables._map.Map], immutables._map.Map]) -> immutables._map.Map`

Apply an arbitrary transformation to the whole map, atomically.

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


### `set(self, key: str, value: Any) -> None`

Set a single key, atomically.

Args:
    key: The key to set.
    value: The value to store. Any Python object.

Example:
    >>> store.set("foo", 123)
    >>> store.get("foo")
    123


### `mset(self, mapping: Dict[str, Any]) -> immutables._map.Map`

Set multiple keys in a **single atomic swap**.

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


### `delete(self, key: str, _event: str = 'delete', _only_if: Optional[Callable[[], bool]] = None) -> bool`

Delete a key if present.

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


### `subscribe(self, fn: Callable[[str, str, Any, Any], NoneType], key: Optional[str] = None) -> nimbuskv.modules.reactive_core.Subscription`

Subscribe to changes on this store.

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


## `Subscription`

Handle returned by :meth:`ReactiveStore.subscribe`.

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


### `cancel(self) -> None`

Stop receiving notifications from this subscription. Safe to
call more than once.


## `ConflictRetryExceeded`

Deprecated: never raised since 3.1.0 (writes no longer retry). Kept
so ``except ConflictRetryExceeded`` in existing code still imports.

Originally raised when a mutation could not win the compare-and-swap
race within ``max_retries`` attempts.

This should only happen under pathological write contention -- many
threads hammering the *same* store with expensive mutations
simultaneously. If you hit this in practice, consider batching writes
with :meth:`ReactiveStore.mset` instead of many individual
:meth:`ReactiveStore.set` calls, since batching reduces the number of
separate swap attempts needed.


## `PersistenceBackend` (base class for custom backends)

Abstract interface for write-through persistence + startup
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


### `load_all(self) -> List[Tuple[str, Any, Optional[float]]]`

Return every non-expired ``(key, value, expire_at_or_None)``
currently in the backend. Called exactly once, at
``NimbusKV.__init__``, to rehydrate the in-memory store before
the TTL scheduler starts. Implementations must filter out
already-expired entries themselves -- NimbusKV trusts what
this returns.


### `persist_set(self, key: str, value: Any, expire_at: Optional[float]) -> None`

Write-through mirror of a ``set()``. Called after the
in-memory write has already succeeded -- this is durability,
not the source of truth.

Args:
    key: The key that was set.
    value: The value that was set.
    expire_at: Absolute Unix timestamp the key should expire
        at, or ``None`` for no expiry.


### `persist_delete(self, key: str) -> None`

Write-through mirror of a ``delete()`` (manual or TTL
expiry). Called after the in-memory delete has already
succeeded.


### `close(self) -> None`

Release any resources (connections, file handles). Called
by ``NimbusKV.stop()``. Default implementation does nothing --
override if your backend holds a resource that needs closing.


## `NullBackend`

Pure in-memory mode: no persistence at all. This is the default
backend (``NimbusKV()`` with no ``backend=`` argument) -- nothing
survives a process restart, and ``NimbusKV``'s async methods stay
genuinely non-blocking since there's no real I/O to delegate.


## `SQLiteBackend`

File-backed persistence via ``sqlite3``.

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


### `__init__(self, path: str = 'nimbuskv.db')`

Args:
    path: Filesystem path to the SQLite database file. Created
        if it doesn't exist.


## `RedisBackend`

Redis-backed persistence, using Redis's own native TTL (``EX``)
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


### `__init__(self, url: Optional[str] = None, client: Optional[object] = None, namespace: str = 'nimbuskv:')`

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


### `resolve_backend(backend)`

Turn a ``backend=`` argument into a :class:`PersistenceBackend`
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


## `ExpiryScheduler` (advanced - used internally by `NimbusKV`)

Background thread that deletes keys at their TTL deadline.

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

Example:
    >>> store = ReactiveStore()
    >>> scheduler = ExpiryScheduler(store)
    >>> scheduler.start()
    >>> store.set("temp", "value")
    >>> scheduler.schedule("temp", time.time() + 5)  # expires in 5s


### `__init__(self, store: nimbuskv.modules.reactive_core.ReactiveStore)`

Args:
    store: The :class:`~.reactive_core.ReactiveStore` this
        scheduler will call ``.delete(key, _event="expire")``
        on when a key's TTL elapses.


### `schedule(self, key: str, when: float) -> None`

Schedule ``key`` to expire at absolute time ``when`` (as
returned by ``time.time()``).

Calling this again for the same key before it expires replaces
the previous schedule -- the old deadline is invalidated via
the generation counter, not left to fire alongside the new one.

Args:
    key: The key to expire.
    when: Absolute Unix timestamp (``time.time() + ttl_seconds``)
        at which the key should be deleted.


### `cancel(self, key: str) -> None`

Cancel any pending expiry for ``key`` (e.g. it was
overwritten with no TTL, or deleted manually before its TTL
elapsed). Safe to call even if nothing was scheduled.

Keys that never had a TTL return immediately without taking the
scheduler lock -- this is called on every plain ``set()``.
No wake-up is needed: the stale heap entry is skipped when it
comes up.


## Runtime detection


### `gil_enabled() -> bool`

Return True if the GIL is enabled in this interpreter.

On Python < 3.13, the GIL always exists -> True.
On Python >= 3.13 built with --disable-gil, this reflects the actual
runtime state: the GIL can still be re-enabled at runtime (e.g. by a
C extension that doesn't support free-threading), so this is checked
once at import time and treated as authoritative for this process.


### `is_free_threaded_build() -> bool`

Return True if this CPython binary was built with --disable-gil,
regardless of whether the GIL happens to be enabled right now.


### `LOCK_FREE_MODE: bool`
Computed once at import time from the two functions above. `NimbusKV`'s concurrency strategy is chosen from this flag automatically - you never set it yourself.


## `NimbusKVError`

Base exception for all NimbusKV errors.
