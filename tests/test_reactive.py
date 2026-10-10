# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""test_reactive.py - subscribe() and wait_for() tests."""
import threading
import time

import pytest

from nimbuskv import NimbusKV


def test_subscribe_receives_set_events(ld):
    events = []
    ld.subscribe(lambda k, e, o, n: events.append((k, e, o, n)))

    ld.set("foo", 1)
    ld.set("foo", 2)

    assert events == [
        ("foo", "set", None, 1),
        ("foo", "set", 1, 2),
    ]


def test_subscribe_key_filter(ld):
    events = []
    ld.subscribe(lambda k, e, o, n: events.append(k), key="only_this")

    ld.set("only_this", 1)
    ld.set("not_this", 1)

    assert events == ["only_this"]


def test_subscription_cancel_stops_notifications(ld):
    events = []
    sub = ld.subscribe(lambda k, e, o, n: events.append(k))

    ld.set("a", 1)
    sub.cancel()
    ld.set("b", 1)

    assert events == ["a"]


def test_subscriber_exception_does_not_break_writer(ld):
    """A misbehaving subscriber must not prevent the write from
    succeeding or crash the caller."""

    def bad_subscriber(k, e, o, n):
        raise RuntimeError("boom")

    ld.subscribe(bad_subscriber)
    ld.set("foo", 1)  # must not raise
    assert ld.get("foo") == 1


def test_atomic_fires_subscriber_notification(ld):
    """Regression test: atomic() must notify subscribers, same as
    set(). This was a real bug -- atomic() used to call the store's
    internal mutate() directly, bypassing notification entirely, so
    subscribe()/wait_for() silently never fired for atomic() updates."""
    events = []
    ld.subscribe(lambda k, e, o, n: events.append((k, e, o, n)), key="counter")

    ld.set("counter", 0)
    ld.atomic("counter", lambda v: v + 1)
    ld.atomic("counter", lambda v: v + 1)

    assert events == [
        ("counter", "set", None, 0),
        ("counter", "set", 0, 1),
        ("counter", "set", 1, 2),
    ]


def test_wait_for_unblocks_on_atomic_update(ld):
    """Regression test for the same bug, from the wait_for() side:
    a thread blocked in wait_for() must actually unblock when another
    thread updates the watched key via atomic(), not just via set()."""
    ld.set("counter", 0)
    result = {}

    def waiter():
        result["v"] = ld.wait_for("counter", lambda v: v >= 5, timeout=5)

    t = threading.Thread(target=waiter)
    t.start()
    for _ in range(5):
        ld.atomic("counter", lambda v: v + 1)
    t.join(timeout=5)

    assert result["v"] == 5


def test_wait_for_unblocks_on_condition(ld):
    ld.set("job", "pending")
    result = {}

    def consumer():
        result["v"] = ld.wait_for("job", lambda v: v == "done", timeout=5)

    t = threading.Thread(target=consumer)
    t.start()
    time.sleep(0.2)
    ld.set("job", "done")
    t.join(timeout=5)

    assert result["v"] == "done"


def test_wait_for_returns_immediately_if_already_true(ld):
    ld.set("job", "done")
    result = ld.wait_for("job", lambda v: v == "done", timeout=1)
    assert result == "done"


def test_wait_for_times_out(ld):
    with pytest.raises(TimeoutError):
        ld.wait_for("never_set", lambda v: v == "x", timeout=0.2)
