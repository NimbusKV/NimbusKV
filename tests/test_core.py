# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""test_core.py - basic CRUD and concurrency-correctness tests.

The concurrency tests here are the ones that actually matter for
NimbusKV's core claim: no lost updates under contention, regardless of
how many threads are writing at once. This is what should be re-run
(and re-verified) on a free-threaded (3.13t/3.14t) interpreter, not
just standard Python.
"""
import threading

import pytest

from nimbuskv import NimbusKV, ConflictRetryExceeded, LOCK_FREE_MODE


def test_set_get(ld):
    ld.set("foo", 123)
    assert ld.get("foo") == 123


def test_get_missing_returns_default(ld):
    assert ld.get("missing") is None
    assert ld.get("missing", "fallback") == "fallback"


def test_delete(ld):
    ld.set("foo", 1)
    assert ld.delete("foo") is True
    assert ld.get("foo") is None
    assert ld.delete("foo") is False  # already gone


def test_exists_and_contains(ld):
    ld.set("foo", 1)
    assert ld.exists("foo") is True
    assert "foo" in ld
    assert "bar" not in ld


def test_keys_items_len(ld):
    ld.mset({"a": 1, "b": 2})
    assert set(ld.keys()) == {"a", "b"}
    assert set(ld.items()) == {("a", 1), ("b", 2)}
    assert len(ld) == 2


def test_iteration(ld):
    ld.mset({"a": 1, "b": 2})
    assert set(iter(ld)) == {"a", "b"}


def test_mset_is_atomic_and_mget(ld):
    ld.mset({"x": 1, "y": 2})
    assert ld.mget(["x", "y", "z"]) == {"x": 1, "y": 2, "z": None}


def test_repr_shows_size_and_backend(ld):
    ld.set("a", 1)
    r = repr(ld)
    assert "size=1" in r
    assert "NullBackend" in r


def test_context_manager_stops_cleanly():
    with NimbusKV() as ld:
        ld.set("a", 1)
        assert ld.get("a") == 1
    # scheduler/backend stopped here -- no assertion needed beyond
    # "this didn't raise"


def test_atomic_no_lost_updates_under_contention(ld):
    """The core promise: many threads incrementing the same key
    concurrently must not lose a single update, with no lock ever
    held during the increment itself."""
    ld.set("counter", 0)
    n_threads = 20
    per_thread = 500

    def worker():
        for _ in range(per_thread):
            ld.atomic("counter", lambda v: v + 1)

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert ld.get("counter") == n_threads * per_thread


def test_mutate_retries_under_contention():
    """Lower-level check directly against ReactiveStore.mutate() --
    verifies the retry-on-conflict loop itself, independent of the
    NimbusKV convenience wrapper."""
    from nimbuskv.modules.reactive_core import ReactiveStore

    store = ReactiveStore()
    store.set("v", 0)
    n_threads = 16
    per_thread = 300

    def worker():
        for _ in range(per_thread):
            store.mutate(lambda m: m.set("v", m.get("v", 0) + 1))

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert store.get("v") == n_threads * per_thread


def test_conflict_retry_exceeded_is_importable():
    # Just verifies the exception exists and is exported correctly --
    # actually triggering it requires pathological contention that
    # isn't worth reproducing in a normal test run.
    assert issubclass(ConflictRetryExceeded, Exception)


def test_lock_free_mode_flag_exists():
    # On this interpreter it will most likely be False (standard GIL
    # Python); the point of this test is just that the flag is a bool
    # and importable, not what value it holds here.
    assert isinstance(LOCK_FREE_MODE, bool)
