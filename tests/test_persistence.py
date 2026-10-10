# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""test_persistence.py - persistence backend tests.

These specifically verify the write-through + rehydration contract:
data written by one NimbusKV instance must be loadable by a fresh
instance pointed at the same backend (simulating a process restart),
and reads must never touch the backend directly (memory is always the
live source of truth).
"""
import time

import pytest

from nimbuskv import NimbusKV
from nimbuskv.modules.persistence import RedisBackend


def test_sqlite_rehydration_after_restart(tmp_path):
    db_path = tmp_path / "test.db"

    ld1 = NimbusKV(backend=f"sqlite:{db_path}")
    ld1.set("persisted", {"x": 42})
    ld1.stop()

    ld2 = NimbusKV(backend=f"sqlite:{db_path}")  # simulates a restart
    assert ld2.get("persisted") == {"x": 42}
    ld2.stop()


def test_sqlite_delete_persists_across_restart(tmp_path):
    db_path = tmp_path / "test.db"

    ld1 = NimbusKV(backend=f"sqlite:{db_path}")
    ld1.set("k", "v")
    ld1.delete("k")
    ld1.stop()

    ld2 = NimbusKV(backend=f"sqlite:{db_path}")
    assert ld2.get("k") is None
    ld2.stop()


def test_sqlite_expired_keys_not_rehydrated(tmp_path):
    db_path = tmp_path / "test.db"

    ld1 = NimbusKV(backend=f"sqlite:{db_path}")
    ld1.set("temp", "v", ttl=0.2)
    time.sleep(0.4)  # let it expire in ld1 too, before "restart"
    ld1.stop()

    ld2 = NimbusKV(backend=f"sqlite:{db_path}")
    assert ld2.get("temp") is None
    ld2.stop()


def test_redis_backend_via_fakeredis():
    fakeredis = pytest.importorskip("fakeredis")
    fake_client = fakeredis.FakeRedis()

    backend = RedisBackend(client=fake_client)
    ld1 = NimbusKV(backend=backend)
    ld1.set("r1", [1, 2, 3])
    ld1.stop()

    backend2 = RedisBackend(client=fake_client)  # same underlying fake server
    ld2 = NimbusKV(backend=backend2)
    assert ld2.get("r1") == [1, 2, 3]
    ld2.stop()
