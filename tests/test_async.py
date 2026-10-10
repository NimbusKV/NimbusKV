# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""test_async.py - async API tests.

Verifies aset/aget/adelete/aexists work, and specifically that the
in-memory path is truly non-blocking (no executor thread spawned) --
that's a real architectural claim, not just "it has async methods".
"""
import asyncio
import threading

import pytest

from nimbuskv import NimbusKV


@pytest.mark.asyncio
async def test_async_set_get(ld):
    await ld.aset("foo", 42)
    assert await ld.aget("foo") == 42


@pytest.mark.asyncio
async def test_async_delete_exists(ld):
    await ld.aset("foo", 1)
    assert await ld.aexists("foo") is True
    assert await ld.adelete("foo") is True
    assert await ld.aexists("foo") is False


@pytest.mark.asyncio
async def test_async_memory_path_spawns_no_thread(ld):
    """For the default in-memory backend, async operations must not
    delegate to a thread-pool executor -- there's no real I/O to hide
    behind one. This checks no new thread appears during the call."""
    before = threading.active_count()
    await ld.aset("foo", 1)
    await ld.aget("foo")
    after = threading.active_count()
    assert after == before


@pytest.mark.asyncio
async def test_async_and_sync_share_state(ld):
    ld.set("foo", 1)
    assert await ld.aget("foo") == 1
    await ld.aset("foo", 2)
    assert ld.get("foo") == 2
