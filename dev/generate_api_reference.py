# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""generate_api_reference.py - regenerates dev/API_REFERENCE.md from the
library's own docstrings.

Run this after any public API change:

    python dev/generate_api_reference.py

This exists so API_REFERENCE.md can never silently drift out of sync with
the actual code -- it's not hand-maintained prose, it's extracted directly
from inspect.getdoc() on the real classes/methods.
"""
import inspect

import nimbuskv
from nimbuskv.modules.nimbuskv_v3 import NimbusKV
from nimbuskv.modules.reactive_core import ReactiveStore, Subscription, ConflictRetryExceeded
from nimbuskv.modules.persistence import PersistenceBackend, NullBackend, SQLiteBackend, RedisBackend, resolve_backend
from nimbuskv.modules.ttl import ExpiryScheduler
from nimbuskv.modules.runtime import gil_enabled, is_free_threaded_build
from nimbuskv.modules.exceptions import NimbusKVError

out = []


def header():
    out.append("# NimbusKV API Reference\n")
    out.append(
        "Generated from the library's own docstrings - every public class and "
        "method below is documented at the source; this file exists so you can "
        "browse the full surface in one place without opening five modules. "
        "Regenerate with `python dev/generate_api_reference.py` after any "
        "public API change, so this never drifts from the actual code.\n"
    )


def section(title, obj):
    out.append(f"\n## {title}\n")
    doc = inspect.getdoc(obj)
    if doc:
        out.append(doc + "\n")


def method_doc(cls, name):
    fn = getattr(cls, name)
    sig = inspect.signature(fn)
    out.append(f"\n### `{name}{sig}`\n")
    doc = inspect.getdoc(fn)
    if doc:
        out.append(doc + "\n")


def build():
    header()

    out.append("\n## Package\n")
    out.append(f"`nimbuskv.__version__` = `{nimbuskv.__version__}`\n")
    out.append(f"\nExported names: {', '.join(sorted(nimbuskv.__all__))}\n")

    section("`NimbusKV`", NimbusKV)
    for name in ['__init__', 'set', 'mset', 'get', 'mget', 'delete', 'exists',
                 'keys', 'items', 'subscribe', 'atomic', 'wait_for', 'stop',
                 'aset', 'aget', 'adelete', 'aexists']:
        method_doc(NimbusKV, name)

    section("`ReactiveStore` (advanced - usually accessed via `NimbusKV`, not directly)", ReactiveStore)
    for name in ['get', 'exists', 'keys', 'items', 'snapshot', 'mutate', 'set', 'mset', 'delete', 'subscribe']:
        method_doc(ReactiveStore, name)

    section("`Subscription`", Subscription)
    method_doc(Subscription, 'cancel')

    section("`ConflictRetryExceeded`", ConflictRetryExceeded)

    section("`PersistenceBackend` (base class for custom backends)", PersistenceBackend)
    for name in ['load_all', 'persist_set', 'persist_delete', 'close']:
        method_doc(PersistenceBackend, name)

    section("`NullBackend`", NullBackend)
    section("`SQLiteBackend`", SQLiteBackend)
    method_doc(SQLiteBackend, '__init__')
    section("`RedisBackend`", RedisBackend)
    method_doc(RedisBackend, '__init__')

    out.append("\n### `resolve_backend(backend)`\n")
    out.append(inspect.getdoc(resolve_backend) + "\n")

    section("`ExpiryScheduler` (advanced - used internally by `NimbusKV`)", ExpiryScheduler)
    for name in ['__init__', 'schedule', 'cancel']:
        method_doc(ExpiryScheduler, name)

    out.append("\n## Runtime detection\n")
    out.append("\n### `gil_enabled() -> bool`\n")
    out.append(inspect.getdoc(gil_enabled) + "\n")
    out.append("\n### `is_free_threaded_build() -> bool`\n")
    out.append(inspect.getdoc(is_free_threaded_build) + "\n")
    out.append(
        "\n### `LOCK_FREE_MODE: bool`\nComputed once at import time from the "
        "two functions above. `NimbusKV`'s concurrency strategy is chosen "
        "from this flag automatically - you never set it yourself.\n"
    )

    section("`NimbusKVError`", NimbusKVError)


if __name__ == "__main__":
    build()
    with open("dev/API_REFERENCE.md", "w") as f:
        f.write("\n".join(out))
    print(f"wrote dev/API_REFERENCE.md ({len(''.join(out))} chars)")
