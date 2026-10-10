# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""conftest.py - shared fixtures for the NimbusKV test suite."""
import pytest

from nimbuskv import NimbusKV


@pytest.fixture
def ld():
    """A fresh in-memory NimbusKV instance, stopped automatically after
    the test."""
    instance = NimbusKV()
    yield instance
    instance.stop()


@pytest.fixture
def sqlite_ld(tmp_path):
    """A fresh SQLite-backed NimbusKV instance using a temp file, so
    each test gets an isolated database that pytest cleans up
    automatically."""
    db_path = tmp_path / "test.db"
    instance = NimbusKV(backend=f"sqlite:{db_path}")
    yield instance
    instance.stop()
