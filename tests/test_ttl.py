# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""test_ttl.py - TTL expiry behavior."""
import time

from nimbuskv import NimbusKV


def test_ttl_expires(ld):
    ld.set("temp", "value", ttl=0.2)
    assert ld.get("temp") == "value"
    time.sleep(0.4)
    assert ld.get("temp") is None


def test_expire_event_distinct_from_delete_event(ld):
    events = []
    ld.subscribe(lambda k, e, o, n: events.append(e), key="temp")

    ld.set("temp", "value", ttl=0.2)
    time.sleep(0.4)

    assert "expire" in events
    assert "delete" not in events


def test_manual_delete_cancels_pending_ttl(ld):
    """A manual delete before the TTL fires must not leave a ghost
    'expire' event later."""
    events = []
    ld.subscribe(lambda k, e, o, n: events.append(e), key="temp")

    ld.set("temp", "value", ttl=5)  # long enough not to fire on its own
    ld.delete("temp")
    time.sleep(0.1)

    assert events == ["set", "delete"]


def test_rescheduling_ttl_uses_latest_deadline_not_stale_one(ld):
    """Regression test for the generation-counter fix: setting a key
    with a long TTL, then immediately overwriting it with a short TTL,
    must expire on the SHORT deadline -- and must not also fire a
    second, stale expiry later from the original long-TTL schedule."""
    events = []
    ld.subscribe(lambda k, e, o, n: events.append((e, time.time())), key="flip")

    ld.set("flip", "v1", ttl=10)
    ld.set("flip", "v2", ttl=0.2)  # supersedes the 10s schedule

    time.sleep(0.4)
    assert ld.get("flip") is None
    expire_events = [e for e in events if e[0] == "expire"]
    assert len(expire_events) == 1  # not two -- no stale duplicate


def test_setting_without_ttl_after_ttl_cancels_expiry(ld):
    ld.set("k", "v1", ttl=0.2)
    ld.set("k", "v2")  # no ttl -- should cancel the pending expiry
    time.sleep(0.4)
    assert ld.get("k") == "v2"


def test_mset_with_shared_ttl(ld):
    ld.mset({"a": 1, "b": 2}, ttl=0.2)
    assert ld.get("a") == 1
    assert ld.get("b") == 2
    time.sleep(0.4)
    assert ld.get("a") is None
    assert ld.get("b") is None
