# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""testfile.py - NimbusKV v3 smoke tests.

Rewritten for the v3 API. The old version used work_mode=, register_callback,
client=, and lock()/unlock() -- none of which exist in v3. Run any function
by uncommenting it below.
"""
import asyncio
import time
from nimbuskv import NimbusKV


def test_basic_sync():
    print('\n=== [1] Basic Sync CRUD (Memory Backend) ===')
    live = NimbusKV()  # backend='memory' is the default
    live.set('foo', 123)
    live.set('bar', 'hello')
    print('foo =', live.get('foo'))
    print('bar =', live.get('bar'))
    print("'foo' in live? ->", 'foo' in live)
    print('len(live) =', len(live))
    print('Keys:', list(live))
    print('Items:', live.items())
    live.delete('bar')
    print('After delete, bar =', live.get('bar'))
    live.stop()


def test_ttl():
    print('\n=== [2] TTL / Expiry ===')
    live = NimbusKV()
    live.set('temp', 'expire-me', ttl=2)
    print('Initially:', live.get('temp'))
    time.sleep(3)
    print('After 3s:', live.get('temp'))
    live.stop()


def test_subscriptions():
    # Replaces the old register_callback test. v3 uses subscribe() instead
    # -- one function per subscription (not per event name), and you get
    # (key, event, old_value, new_value) on every set/delete/expire.
    print('\n=== [3] Reactive Subscriptions ===')
    live = NimbusKV()

    def on_any_change(key, event, old, new):
        print(f'[sub] {key} {event}: {old} -> {new}')

    sub = live.subscribe(on_any_change)
    live.set('foo', 'bar', ttl=1)
    time.sleep(2)  # lets the TTL expiry fire and print its own event
    sub.cancel()
    live.set('foo', 'baz')  # no print now -- subscription was cancelled
    live.stop()


def test_atomic():
    # Replaces the old per-key lock()/unlock() test. v3 uses atomic() --
    # no lock is held; it's a safe retry-based compound update instead.
    print('\n=== [4] Atomic Compound Update ===')
    live = NimbusKV()
    live.set('counter', 1)
    new_val = live.atomic('counter', lambda v: v + 1)
    print('Atomic update done, new value:', new_val)
    print('Final value:', live.get('counter'))
    live.stop()


async def test_async_memory():
    print('\n=== [5] Async Usage (Memory Backend) ===')
    live = NimbusKV()
    await live.aset('a', 10, ttl=3)
    print('a =', await live.aget('a'))

    events = []
    live.subscribe(lambda k, e, o, n: events.append((k, e)), key='a')
    await asyncio.sleep(3.5)
    print('events on a:', events)  # should include ('a', 'expire')
    live.stop()


def test_sqlite_sync():
    print('\n=== [6] SQLite Backend (Sync) ===')
    live = NimbusKV(backend='sqlite:test.db')  # path after the colon
    live.set('sqlkey', 'sqlite_value')
    print('sqlkey =', live.get('sqlkey'))
    live.stop()


async def test_sqlite_async():
    print('\n=== [6] SQLite Backend (Async) ===')
    live = NimbusKV(backend='sqlite:test_async.db')
    await live.aset('sqlkey', 'sqlite_value_async')
    print('sqlkey =', await live.aget('sqlkey'))
    live.stop()


def test_redis_sync():
    print('\n=== [7] Redis Backend (Sync) ===')
    try:
        live = NimbusKV(backend='redis://127.0.0.1:6379')
        live.set('redkey', 'redis_value')
        print('redkey =', live.get('redkey'))
        live.stop()
    except Exception as e:
        print('Redis not available:', e)


async def test_redis_async():
    print('\n=== [7] Redis Backend (Async) ===')
    try:
        live = NimbusKV(backend='redis://127.0.0.1:6379')
        await live.aset('redkey', 'redis_value_async')
        print('redkey =', await live.aget('redkey'))
        live.stop()
    except Exception as e:
        print('Redis not available:', e)


def test_benchmark():
    """Benchmark: OLD-style (one lock held for the WHOLE operation, incl.
    the expensive part) vs NEW-style (optimistic, lock only around the
    final pointer swap). On standard GIL Python these come out close --
    the GIL already serializes CPU-bound bytecode, so there's little to
    win. On free-threaded Python (3.13t/3.14t), OLD stays fully
    serialized no matter what, while NEW lets the expensive part run on
    separate cores in parallel, serializing only the swap -- that gap is
    the actual number to put in the README.
    """
    import threading
    from nimbuskv.modules.reactive_core import ReactiveStore
    from nimbuskv import LOCK_FREE_MODE, is_free_threaded_build, gil_enabled

    print('\n=== [8] Benchmark: OLD full-lock vs NEW optimistic-swap ===')
    print('is_free_threaded_build():', is_free_threaded_build())
    print('gil_enabled():', gil_enabled())
    print('LOCK_FREE_MODE:', LOCK_FREE_MODE)

    def expensive_compute(x):
        # stand-in for real per-write work: validation, transform, etc.
        s = 0
        for i in range(2000):
            s += i
        return x + 1

    # --- OLD-style: one lock held for the ENTIRE operation ---
    old_lock = threading.Lock()
    old_counter = {'v': 0}

    def old_worker(n):
        for _ in range(n):
            with old_lock:
                old_counter['v'] = expensive_compute(old_counter['v'])

    # --- NEW-style: optimistic, lock only around the swap ---
    store = ReactiveStore()
    store.set('v', 0)

    def new_worker(n):
        for _ in range(n):
            store.mutate(lambda m: m.set('v', expensive_compute(m.get('v', 0))))

    N_THREADS = 8
    PER_THREAD = 300

    t0 = time.perf_counter()
    ts = [threading.Thread(target=old_worker, args=(PER_THREAD,)) for _ in range(N_THREADS)]
    for t in ts: t.start()
    for t in ts: t.join()
    t1 = time.perf_counter()
    old_time = t1 - t0
    print(f'OLD (global lock, full-op serialized):     {old_time:.3f}s  final={old_counter["v"]}')

    t0 = time.perf_counter()
    ts = [threading.Thread(target=new_worker, args=(PER_THREAD,)) for _ in range(N_THREADS)]
    for t in ts: t.start()
    for t in ts: t.join()
    t1 = time.perf_counter()
    new_time = t1 - t0
    print(f'NEW (optimistic, swap-only-serialized):    {new_time:.3f}s  final={store.get("v")}')

    speedup = old_time / new_time if new_time > 0 else float('inf')
    print(f'Speedup: {speedup:.2f}x')
    if not LOCK_FREE_MODE:
        print('NOTE: LOCK_FREE_MODE is False on this interpreter (standard GIL '
              'Python) -- expect a small/negligible gap here. Run this on a '
              'free-threaded 3.13t/3.14t build for the real parallel-throughput '
              'numbers.')
    else:
        print('Running in LOCK_FREE_MODE -- this speedup number is the real one '
              'for the README/benchmarks.')


def test_new_features():
    print('\n=== [9] New in this build: context manager, mset/mget, wait_for ===')
    with NimbusKV() as ld:  # context manager -- auto-stops on exit
        ld.mset({'a': 1, 'b': 2})  # atomic multi-key set, one swap
        print('mget:', ld.mget(['a', 'b', 'missing']))
        print(repr(ld))

        # wait_for: block until a condition holds, no polling loop --
        # driven by the subscription/notification system underneath.
        import threading
        result = {}

        def consumer():
            result['status'] = ld.wait_for('job', lambda v: v == 'done', timeout=5)

        t = threading.Thread(target=consumer)
        t.start()
        time.sleep(0.2)
        ld.set('job', 'done')  # consumer's wait_for() unblocks right here
        t.join()
        print('wait_for result:', result['status'])
    # backend closed and scheduler stopped automatically here


if __name__ == '__main__':
    test_basic_sync()
    test_ttl()
    test_subscriptions()
    test_atomic()
    asyncio.run(test_async_memory())
    test_sqlite_sync()
    asyncio.run(test_sqlite_async())
    # test_redis_sync()               # needs a real Redis server
    # asyncio.run(test_redis_async())  # needs a real Redis server
    test_new_features()
    test_benchmark()
