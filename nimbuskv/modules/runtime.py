# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""runtime.py - NimbusKV v3.

Detects the interpreter's concurrency model (standard GIL vs free-threaded)
so the store can adapt its concurrency strategy at import time, once, without
branching on every operation.
"""
import sys
import sysconfig


def gil_enabled() -> bool:
    """Return True if the GIL is enabled in this interpreter.

    On Python < 3.13, the GIL always exists -> True.
    On Python >= 3.13 built with --disable-gil, this reflects the actual
    runtime state: the GIL can still be re-enabled at runtime (e.g. by a
    C extension that doesn't support free-threading), so this is checked
    once at import time and treated as authoritative for this process.
    """
    fn = getattr(sys, "_is_gil_enabled", None)
    if fn is not None:
        return bool(fn())
    # Python < 3.13: no free-threaded builds exist, GIL is always present.
    return True


def is_free_threaded_build() -> bool:
    """Return True if this CPython binary was built with --disable-gil,
    regardless of whether the GIL happens to be enabled right now."""
    return bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


# Computed once at import time. The store picks its concurrency strategy
# from this flag; it does not re-check per-operation.
LOCK_FREE_MODE = is_free_threaded_build() and not gil_enabled()
