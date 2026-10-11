# Changelog

## [3.1.1] - 2026-10-11

Fixed
* **TTL scheduler heap could still grow without bound for a hot key.** The 3.1.0 fix below stopped the scheduler from keeping an entry for every key ever written, but `schedule()` always pushed a new heap entry without removing the old one for the same key. A key with a long TTL that gets rescheduled often (`set(k, v, ttl=...)` called repeatedly on one key) left stale entries sitting in the heap until each one's original deadline arrived, so memory still grew in the meantime even though `_generation` stayed correctly sized. The heap is now compacted once stale entries build up to roughly twice the live count.

Found during code review of v3.1.0 before it was tagged or published. Covered by a new regression test in `tests/test_ttl.py` (5,000 reschedules of one key; heap stays under 200 entries).

## [3.1.0] - 2026-10-10

**License change: NimbusKV is now licensed under the Apache License 2.0** (previously GPL-3.0). You can use it in commercial and closed-source products; redistributions must keep the `LICENSE` and `NOTICE` files. Versions up to and including 3.0.1 remain under GPL-3.0 for anyone who already has them. New files: `NOTICE`, `CLA.md` (contributor license agreement, required for pull requests), `TRADEMARKS.md`, `COMMERCIAL-LICENSING.md`. Every source file now carries an SPDX license header. Contributors are now credited in `CONTRIBUTORS.md` and next to their changes in this changelog.

**Upgrade note:** code that writes to the store from *inside* an `atomic()` or `mutate()` function worked in 3.0.1 and now raises `ReentrantWriteError`. Move the second write after the call, or combine both into one `mutate()`/`mset()`.

Changed
* **Writes now use one short mutex instead of optimistic compare-and-swap with retry.** In v3.0.1, `mutate()` built the new snapshot outside the lock and retried immediately when another writer got there first. Under a hot key every losing thread threw its work away and tried again, so throughput fell as threads were added. Each write now takes the lock once and does its work once. Readers are unaffected and still never lock.
* **`atomic()` / `mutate()` functions now run exactly once per call** (they used to re-run on every lost race), while the write lock is held. Writing to the same store from inside one now raises the new `ReentrantWriteError` instead of deadlocking.
* `max_retries` and `ConflictRetryExceeded` are kept for compatibility but are no longer used.

Fixed
* **`delete()` could report a deletion that did not happen.** Two threads deleting the same key could both return `True` and both fire a `"delete"` notification. The existence check now happens inside the write, so exactly one wins.
* **Subscribers could be told the wrong old value.** `set()`, `mset()` and `delete()` read the old value before the write, so under concurrent writers it could be stale. It is now captured inside the write.
* **A TTL expiry could delete a value written after it.** The scheduler checked "is this TTL still live?", released its lock, then deleted. A `set()` in that gap was wiped out. The check now runs inside the store's write lock, and `set()` cancels the old TTL before writing.
* **TTL scheduler memory leak.** The scheduler kept a generation entry for every key ever written, with or without a TTL, forever. Entries now exist only while a TTL is pending. (The underlying heap itself could still grow unbounded for a rescheduled hot key; fixed in 3.1.1.)
* **`atomic()` dropped a key's TTL in the persistence backend.** It persisted `expire_at=None`, so a TTL key touched by `atomic()` never expired after a restart. It now persists the real pending expiry.
* Issue templates were in `.github/ISSSUE_TEMPLATE/` (typo), so GitHub ignored them. Moved to `.github/ISSUE_TEMPLATE/`, root duplicates removed, PR template moved into `.github/`.
* Free-threaded CI checked the GIL before importing NimbusKV, so it missed that importing `immutables` turns the GIL back on. It now checks after the import and runs with `PYTHON_GIL=0`.

Performance
* `set()` without subscribers or TTL no longer takes the subscriber lock or the TTL scheduler lock.
* `_notify()` takes no lock at all (subscriber lists are copy-on-write).
* Measured with `bench/b06_write_path.py` on a 2-core Linux sandbox (median of 5):

| | 3.0.1 | 3.1.0 |
|---|---|---|
| `set()`, GIL 3.13 | 328K ops/s | 485K ops/s (+48%) |
| `set()`, free-threaded 3.14t (`gil=0`) | 188K ops/s | 483K ops/s (+157%) |
| `set()` + 1 subscriber, GIL 3.13 | 304K ops/s | 438K ops/s (+44%) |
| hot-key `atomic()`, 1-32 threads | roughly flat | roughly flat, within noise |

The hot-key collapse in the published 3.0.1 benchmark (439K to 55K ops/s, 16 logical CPUs) could not be reproduced on 2 cores, so 3.1.0's effect on it is not yet measured. Run `bench/b06_write_path.py` on a many-core machine before claiming it.

Added
* `tests/test_concurrency.py`: 11 regression tests, one per bug above. 8 of them fail on 3.0.1.
* `bench/b06_write_path.py`: write-path before/after benchmark.

## [3.0.1] - 2026-08-11

Fixed
* **`atomic()` did not notify subscribers.** `NimbusKV.atomic()` was calling the store's internal `mutate()` directly, bypassing the notification step entirely - so `subscribe()` and anything built on it (including `wait_for()`) silently never fired for changes made via `atomic()`, even though the value itself updated correctly. Fixed by giving `ReactiveStore` its own `atomic()` that performs the read-modify-write and the notification as a single operation; `NimbusKV.atomic()` now delegates to it. Covered by regression tests in `tests/test_reactive.py`.

Added
* `tests/` - pytest suite (36 tests) covering core CRUD, concurrency correctness, TTL, reactive subscriptions, async, and all persistence backends.
* `.github/workflows/tests.yml` - CI across standard Python 3.9–3.13 and free-threaded 3.13t/3.14t.
* `.github/FUNDING.yml`, `CONTRIBUTING.md`.
* `dev/API_REFERENCE.md`, generated from the library's own docstrings via `dev/generate_api_reference.py`.
* `examples/session_store.py`, `examples/worker_pool.py` - realistic end-to-end usage.
* `pyproject.toml` at the repo root (previously missing entirely - `pip install -e .` did not work from a fresh clone).

## [3.0.0] - 2026-08-10

**Project renamed from `livedict` to `nimbuskv`.** The import path is now `from nimbuskv import NimbusKV` (previously `from livedict import LiveDict`). See the migration note in the README for details. The old `livedict` package on PyPI will remain published as a deprecated stub pointing here.

**Rewrite - new architecture, not a patch on v2.**

Added
* `ReactiveStore`: immutable-snapshot, optimistic-concurrency core (lock-free reads always; lock-free writes on free-threaded Python via runtime detection, correct retry-based writes on standard GIL Python).
* `runtime.py`: one-time interpreter detection (`LOCK_FREE_MODE`, `gil_enabled()`, `is_free_threaded_build()`).
* `subscribe()`: real push-based reactivity (`set`/`delete`/`expire` events), replacing v2's fire-and-forget callback registration.
* `atomic()`: safe compound read-modify-write, replacing manual `lock()`/`unlock()`.
* `mset()`/`mget()`: batched multi-key atomic write + convenience multi-key read.
* `wait_for()`: block a thread until a condition on a key holds, via subscriptions -- no polling.
* Context manager support (`with NimbusKV() as ld:`).
* `py.typed` marker (PEP 561) for type-checker support.
* Genuinely non-blocking async API for the in-memory path (no `run_in_executor` used when there's no real I/O).
* Persistence backends (`NullBackend`/`SQLiteBackend`/`RedisBackend`) rebuilt as write-through mirrors with startup rehydration, decoupled from the hot read/write path.
* Full docstrings (Google-style, with runnable examples) on every public class and method.

Fixed
* `ExpiryScheduler` now uses a per-key generation counter -- rescheduling a key's TTL correctly invalidates the previous deadline instead of leaving a stale heap entry that could (harmlessly, but wastefully) fire later.
* Subscriber callbacks that raise no longer propagate into the write path or the scheduler thread; they're logged via the `nimbuskv` logger instead.

Removed
* Sandbox module - v2's was a timeout wrapper, not real isolation; removed rather than kept under a misleading name. Planned for a future release with actual isolation.
* Manual `lock()`/`unlock()` - replaced by `atomic()`.
* Fake-async (`run_in_executor` wrapping non-I/O calls) for the memory backend.

Breaking
* This is not backwards compatible with v2's API by design (`work_mode=` constructor arg removed in favor of `set`/`aset` naming; callback registration replaced by `subscribe()`).


All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project follows Semantic Versioning.

## Release: [2.0.2] - 2025-10-05 - latest
### Changed
- The callback handler was calling the callbacks 2 times.I messed up by adding another loop if there is any RunTimeError at _run_async in the _trigger_event function.. fixed it.

## Release: [2.0.1] - 2025-10-05
### Changed
- setup.py had a major requirement issue that was fixed. (I had mistakenly added sqlite3 as a requirement but it's not needed since Python 3.6+ has included it.)

## Release: [2.0.0] - 2025-10-04
### Added
- Comprehensive Google-style docstrings added to modules, classes, and functions.
- Sandbox functionality is now active and maintained within the package.

### Changed
- Project version bumped to `2.0.0`.
- Packaging files (`setup.py`, `pyproject.toml`) updated for PyPI release.

### Removed
- Cryptography-related code has been removed from the package. Encryption and cryptographic responsibilities are now delegated to the user; the library no longer provides built-in cryptographic primitives.

### Notes
- The `sandbox` module is live and behaves according to the code in this release; users should review sandbox behavior for their environment and use cases.

## Release: [v1.0.4] - 2025-08-11

### Added
* Pydantic-based configuration models for storage/backends/monitoring and bucket policies.
* New persistence backends:
  - SQLite backend with short-lived connections and indexing.
  - File-backed object store for external object storage emulation.
  - Improved InMemory backend used for testing and simple runs.
* CipherAdapter with AES-GCM (cryptography) and deterministic base64 XOR fallback.
* Heap-based expiry monitor with efficient wake-ups and rebuild heuristics.
* Bucket semantics, bucket policies, and hybrid routing placeholders.
* Better backend fallbacks and tolerant lookup behavior (any-bucket fallback for SQLite/File).
* Expanded API and documentation covering rotation, hooks, and persistence options.

### Changed
* Packaging bumped to `1.0.4-release` to mark the release-ready core implementation.
* README and packaging metadata updated to reflect new dependencies and features.

### Fixed
* Backend expiry handling improved to avoid stale reads.
* sqlite backend uses short-lived sqlite3 connections to avoid file locks on Windows.
* File backend tolerant lookup fixes and safer cleanup logic.

---
## Prior patch notes (v1.0.4 and earlier)
No previous changes. This is the first version of NimbusKV's official release.
