# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""test_concurrency.py - regression tests for the 3.1.0 write-path fixes.

Every test here targets a specific bug found in v3.0.1. Each one fails
(or can fail) against v3.0.1 and passes after the fix, so they guard
against the bug coming back.

Races are only a few bytecodes wide, so the tests that need a specific
interleaving shrink the thread switch interval and line threads up on
a Barrier. On free-threaded Python (run with ``-X gil=0``) they get
real parallelism instead.
"""
import sys
import threading
import time

import pytest

from nimbuskv import NimbusKV
from nimbuskv.modules.reactive_core import ReactiveStore, ReentrantWriteError


@pytest.fixture
def fast_switch():
    old = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    yield
    sys.setswitchinterval(old)


def _join_all(threads, timeout=30.0):
    deadline = time.time() + timeout
    for t in threads:
        t.join(max(0.0, deadline - time.time()))
        assert not t.is_alive(), "thread hung (possible deadlock)"


# ---- Phase 1: mutate() is a plain mutex, no retry storm ---------------

def test_hot_key_atomic_no_lost_updates():
    ld = NimbusKV()
    try:
        threads, per = 64, 500
        ld.set("counter", 0)

        def worker():
            for _ in range(per):
                ld.atomic("counter", lambda v: v + 1)

        ts = [threading.Thread(target=worker, daemon=True) for _ in range(threads)]
        for t in ts:
            t.start()
        _join_all(ts)
        assert ld.get("counter") == threads * per
    finally:
        ld.stop()


def test_atomic_fn_runs_exactly_once_per_call(fast_switch):
    # v3.0.1 re-ran fn on every lost CAS race. Now it runs once.
    store = ReactiveStore()
    store.set("c", 0)
    calls = []
    lock = threading.Lock()

    def incr(v):
        with lock:
            calls.append(1)
        return v + 1

    n_threads, per = 16, 200
    barrier = threading.Barrier(n_threads)

    def worker():
        barrier.wait()
        for _ in range(per):
            store.atomic("c", incr)

    ts = [threading.Thread(target=worker, daemon=True) for _ in range(n_threads)]
    for t in ts:
        t.start()
    _join_all(ts)
    assert store.get("c") == n_threads * per
    assert len(calls) == n_threads * per


def test_exception_in_atomic_fn_does_not_wedge_store():
    store = ReactiveStore()
    store.set("k", 1)
    with pytest.raises(ZeroDivisionError):
        store.atomic("k", lambda v: v / 0)
    assert store.get("k") == 1
    store.set("k", 2)  # lock must have been released
    assert store.get("k") == 2


def test_writing_from_inside_atomic_raises_instead_of_deadlocking():
    store = ReactiveStore()
    store.set("a", 1)
    with pytest.raises(ReentrantWriteError):
        store.atomic("a", lambda v: store.set("b", v) or v)
    store.set("a", 5)  # still usable afterwards
    assert store.get("a") == 5


# ---- Phase 2c: delete() race fires exactly one notification ----------

def test_concurrent_delete_exactly_one_winner(fast_switch):
    for i in range(1500):
        store = ReactiveStore()
        store.set("k", i)
        events = []
        store.subscribe(lambda k, e, o, n: events.append(e), key="k")
        results = []
        barrier = threading.Barrier(2)

        def deleter():
            barrier.wait()
            results.append(store.delete("k"))

        ts = [threading.Thread(target=deleter, daemon=True) for _ in range(2)]
        for t in ts:
            t.start()
        _join_all(ts, timeout=5)
        assert sorted(results) == [False, True], f"iteration {i}: {results}"
        assert events == ["delete"], f"iteration {i}: {events}"


def test_set_notification_reports_true_old_value(fast_switch):
    # Each notification's old value must be the value its own write
    # replaced. With stale pre-reads, two concurrent sets could both
    # report the same old value.
    for _ in range(300):
        store = ReactiveStore()
        store.set("k", 0)
        seen = []
        store.subscribe(lambda k, e, o, n: seen.append((o, n)), key="k")
        barrier = threading.Barrier(4)

        def writer(v):
            barrier.wait()
            store.set("k", v)

        ts = [threading.Thread(target=writer, args=(v,), daemon=True) for v in (1, 2, 3, 4)]
        for t in ts:
            t.start()
        _join_all(ts, timeout=5)
        olds = [o for o, _ in seen]
        assert len(set(olds)) == 4, f"duplicate old values reported: {seen}"


# ---- Phase 2a: subscriptions still correct with lock-free notify -----

def test_subscribe_unsubscribe_under_concurrent_writes():
    store = ReactiveStore()
    stop = threading.Event()

    def writer():
        i = 0
        while not stop.is_set():
            store.set("k", i)
            i += 1

    ts = [threading.Thread(target=writer, daemon=True) for _ in range(4)]
    for t in ts:
        t.start()
    try:
        for _ in range(300):
            got = []
            sub = store.subscribe(lambda k, e, o, n: got.append(n), key="k")
            time.sleep(0.0005)
            sub.cancel()
            n_after_cancel = len(got)
            time.sleep(0.0005)
            # At most a write already mid-notify may still land.
            assert len(got) <= n_after_cancel + len(ts)
    finally:
        stop.set()
        _join_all(ts)


# ---- Phase 2b: TTL scheduler no longer leaks, no stale-expiry race ---

def test_ttl_scheduler_does_not_grow_for_keys_without_ttl():
    ld = NimbusKV()
    try:
        for i in range(10_000):
            ld.set(f"k{i}", i)
        for i in range(10_000):
            ld.delete(f"k{i}")
        assert len(ld._scheduler._generation) == 0
    finally:
        ld.stop()


def test_ttl_scheduler_forgets_keys_after_they_expire():
    ld = NimbusKV()
    try:
        for i in range(200):
            ld.set(f"t{i}", i, ttl=0.01)
        deadline = time.time() + 5
        while len(ld) and time.time() < deadline:
            time.sleep(0.02)
        assert len(ld) == 0
        assert len(ld._scheduler._generation) == 0
    finally:
        ld.stop()


def test_overwrite_without_ttl_is_never_expired_by_old_ttl(fast_switch):
    # set(k, v, ttl=tiny) then set(k, v2): the old TTL must never delete v2.
    ld = NimbusKV()
    try:
        for i in range(3000):
            key = f"race{i % 50}"
            ld.set(key, "old", ttl=0.0005)
            time.sleep(0.0004)
            ld.set(key, i)
        time.sleep(0.05)
        for j in range(50):
            assert ld.get(f"race{j}") is not None, f"race{j} wrongly expired"
    finally:
        ld.stop()


def test_atomic_keeps_ttl_in_backend(tmp_path):
    db = tmp_path / "ttl.db"
    ld = NimbusKV(backend=f"sqlite:{db}")
    ld.set("k", 1, ttl=100)
    ld.atomic("k", lambda v: v + 1)
    ld.stop()

    ld2 = NimbusKV(backend=f"sqlite:{db}")
    try:
        assert ld2.get("k") == 2
        assert "k" in ld2._scheduler._generation, "TTL was lost on rehydrate"
    finally:
        ld2.stop()
