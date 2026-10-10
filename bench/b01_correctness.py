# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""b01_correctness.py

Before measuring speed, prove the safety claims. A fast-but-wrong store is
worthless, so this file is charted too (as a pass/fail + retry-count bar),
not just narrated.

Claims under test:
1. atomic() never loses an update under concurrent writers (the whole
   point of optimistic-concurrency vs a plain dict+lock).
2. mset() is truly atomic: a concurrent reader never observes a partial
   multi-key update (the "move money between two accounts" example from
   the README, actually enforced under a tight reader/writer race).
3. ConflictRetryExceeded is a real, reachable failure mode, not just
   documentation -- and it fires exactly where the retry budget says it
   should.
4. TTL expiry vs. manual delete/reschedule races don't double-fire or
   resurrect a key (the generation-counter bug the docs mention fixing).
"""
import threading
import time

from nimbuskv.modules.reactive_core import ReactiveStore, ConflictRetryExceeded
from nimbuskv import NimbusKV

from common import log, save_chart, md_table

SUITE_ID = "b01_correctness"
TITLE = "1. Correctness First"


def test_atomic_no_lost_updates(n_threads=32, incs_per_thread=2000):
    store = ReactiveStore()
    store.set("counter", 0)
    barrier = threading.Barrier(n_threads)

    def worker():
        barrier.wait()
        for _ in range(incs_per_thread):
            store.atomic("counter", lambda v: v + 1)

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t0

    expected = n_threads * incs_per_thread
    actual = store.get("counter")
    return {
        "name": "atomic_no_lost_updates",
        "n_threads": n_threads,
        "incs_per_thread": incs_per_thread,
        "expected": expected,
        "actual": actual,
        "passed": expected == actual,
        "elapsed_s": elapsed,
        "ops_per_sec": expected / elapsed,
    }


def _mset_atomicity_run(n_readers, writer_iters, reader_reads_via_snapshot):
    """Shared driver. Two accounts must always sum to 200. A reader racing
    a writer that swings money between them must never observe a moment
    where the sum is anything else -- *if* the reader takes a single
    consistent view. If reader_reads_via_snapshot is False, the reader
    calls get() twice (two independent lock-free reads), which can
    straddle a writer's swap and is NOT the same guarantee as mset()
    itself being atomic."""
    store = ReactiveStore()
    store.mset({"account_a": 100, "account_b": 100})
    violations = []
    stop = threading.Event()
    lock = threading.Lock()

    def reader():
        while not stop.is_set():
            if reader_reads_via_snapshot:
                snap = store.snapshot()
                a, b = snap["account_a"], snap["account_b"]
            else:
                a = store.get("account_a")
                b = store.get("account_b")
            if a + b != 200:
                with lock:
                    violations.append((a, b))

    readers = [threading.Thread(target=reader) for _ in range(n_readers)]
    for r in readers:
        r.start()

    t0 = time.perf_counter()
    for i in range(writer_iters):
        delta = 10 if i % 2 == 0 else -10
        a = store.get("account_a")
        store.mset({"account_a": a + delta, "account_b": 200 - (a + delta)})
    elapsed = time.perf_counter() - t0

    stop.set()
    for r in readers:
        r.join()

    return violations, elapsed


def test_mset_atomicity_naive_get_get(n_readers=8, writer_iters=20000):
    """DOCUMENTED GOTCHA, not a library bug: two separate store.get() calls
    are two independent lock-free reads and can straddle a writer's
    snapshot swap. This is expected to show violations -- it demonstrates
    that mset()'s atomicity guarantee covers the *write*, not an implicit
    multi-key *read* done via repeated get()."""
    violations, elapsed = _mset_atomicity_run(n_readers, writer_iters, False)
    return {
        "name": "mset_atomicity_naive_get_get (expected gotcha)",
        "n_readers": n_readers,
        "writer_iters": writer_iters,
        "violations": len(violations),
        "passed": len(violations) > 0,  # we EXPECT to catch the gotcha
        "elapsed_s": elapsed,
        "writes_per_sec": writer_iters / elapsed,
        "note": "get()+get() is two independent reads; use snapshot() for a consistent multi-key view",
    }


def test_mset_atomicity_via_snapshot(n_readers=8, writer_iters=20000):
    """The correct proof of mset() atomicity: a reader takes ONE snapshot
    and reads both keys from it. This must never show a violation."""
    violations, elapsed = _mset_atomicity_run(n_readers, writer_iters, True)
    return {
        "name": "mset_atomicity_via_snapshot",
        "n_readers": n_readers,
        "writer_iters": writer_iters,
        "violations": len(violations),
        "passed": len(violations) == 0,
        "elapsed_s": elapsed,
        "writes_per_sec": writer_iters / elapsed,
    }


def test_conflict_retry_exceeded():
    """The same setup that used to starve v3.0.1's retry budget: tiny
    max_retries, a slow mutator, 24 writers on one key. Since 3.1.0 writes
    take a lock instead of retrying, so every write must succeed and
    none may be lost. (Up to v3.0.1 this check passed when
    ConflictRetryExceeded *did* fire.)"""
    store = ReactiveStore(max_retries=2)
    store.set("hot", 0)
    exceptions = []
    lock = threading.Lock()
    barrier = threading.Barrier(24)

    def slow_mutate(m):
        v = m.get("hot", 0)
        time.sleep(0.0005)  # widen the race window so retries actually collide
        return m.set("hot", v + 1)

    def worker():
        barrier.wait()
        try:
            store.mutate(slow_mutate)
        except ConflictRetryExceeded as e:
            with lock:
                exceptions.append(str(e))

    threads = [threading.Thread(target=worker) for _ in range(24)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    final = store.get("hot")
    return {
        "name": "no_write_fails_under_starved_contention",
        "max_retries_configured": 2,
        "n_writers": 24,
        "n_that_hit_retry_exceeded": len(exceptions),
        "final_value": final,
        "passed": len(exceptions) == 0 and final == 24,
    }


def test_ttl_reschedule_no_resurrection():
    """set with ttl=10 (long), then immediately reschedule to ttl=0.05
    (short). The stale long-deadline heap entry must not resurrect the
    key or double-delete after the short one already fired."""
    ld = NimbusKV()
    try:
        results = []
        for _ in range(200):
            ld.set("k", "v1", ttl=10)
            ld.set("k", "v2", ttl=0.05)
            time.sleep(0.08)
            results.append(ld.get("k"))
        bad = [r for r in results if r is not None]
        return {
            "name": "ttl_reschedule_no_resurrection",
            "trials": len(results),
            "unexpected_alive_after_short_ttl": len(bad),
            "passed": len(bad) == 0,
        }
    finally:
        ld.stop()


def test_subscriber_exception_does_not_break_write():
    ld = NimbusKV()
    try:
        def bad_sub(k, e, o, n):
            raise RuntimeError("boom")

        ld.subscribe(bad_sub)
        ok = True
        try:
            for i in range(500):
                ld.set(f"k{i}", i)
        except Exception:
            ok = False
        return {
            "name": "subscriber_exception_isolated",
            "writes_survived_bad_subscriber": ok,
            "passed": ok and len(ld) == 500,
        }
    finally:
        ld.stop()


def run():
    log("Running correctness suite...")
    results = [
        test_atomic_no_lost_updates(),
        test_mset_atomicity_naive_get_get(),
        test_mset_atomicity_via_snapshot(),
        test_conflict_retry_exceeded(),
        test_ttl_reschedule_no_resurrection(),
        test_subscriber_exception_does_not_break_write(),
    ]
    for r in results:
        status = "PASS" if r["passed"] else "FAIL"
        log(f"  [{status}] {r['name']}")
    return results


def render(results, charts_dir):
    import matplotlib.pyplot as plt
    names = [r["name"] for r in results]
    fig, ax = plt.subplots(figsize=(9, 4))
    colors = ["#10b981" if r["passed"] else "#ef4444" for r in results]
    y = range(len(names))
    ax.barh(y, [1] * len(names), color=colors)
    ax.set_yticks(y)
    ax.set_yticklabels(names, fontsize=8)
    ax.set_xlim(0, 1.3)
    ax.set_xticks([])
    for i, r in enumerate(results):
        label = "PASS" if r["passed"] else "FAIL"
        ax.text(1.02, i, label, va="center", fontsize=9, fontweight="bold",
                color="#10b981" if r["passed"] else "#ef4444")
    ax.set_title("Correctness / safety suite results\n"
                  "(includes a deliberately-caught gotcha: repeated get() is\n"
                  "not the same guarantee as an atomic multi-key read)")
    path = save_chart(fig, "01_correctness_results.png")
    return {"results": path}


def to_markdown(results, charts):
    n_pass = sum(1 for r in results if r["passed"])
    by_name = {r["name"]: r for r in results}

    lines = []
    lines.append(f"## {TITLE}\n")
    lines.append(
        "A fast key-value store that loses updates or corrupts state under "
        "load is worthless regardless of its throughput numbers, so before "
        f"any performance number counted for anything, **{len(results)} "
        f"adversarial correctness tests** had to pass. Result: "
        f"**{n_pass}/{len(results)} passed.**\n"
    )
    lines.append(f'![Correctness results]({charts["results"]})')
    lines.append("")

    rows = []
    a = by_name.get("atomic_no_lost_updates")
    if a:
        rows.append([
            "No lost updates",
            f"{a['n_threads']} threads x {a['incs_per_thread']} `atomic()` increments on one shared counter",
            f"**{a['actual']:,}/{a['expected']:,}** -- {'zero lost writes' if a['passed'] else 'LOST WRITES DETECTED'}",
        ])
    naive = by_name.get("mset_atomicity_naive_get_get (expected gotcha)")
    if naive:
        rows.append([
            "`mset()` naive-read gotcha",
            f"Two separate `get()` calls racing a writer swinging money between two keys ({naive['writer_iters']:,} writes)",
            f"**Caught on purpose** -- {naive['violations']:,} violations observed. "
            f"Not a bug: `get()`+`get()` is two independent lock-free reads.",
        ])
    snap = by_name.get("mset_atomicity_via_snapshot")
    if snap:
        rows.append([
            "`mset()` atomicity, done right",
            f"Same race, but the reader takes one `snapshot()` and reads both keys from it ({snap['writer_iters']:,} writes)",
            f"**{snap['violations']} violations** across {snap['writer_iters']:,} writes -- this is the real proof of atomicity",
        ])
    cre = by_name.get("no_write_fails_under_starved_contention")
    if cre:
        rows.append([
            "No write fails under starved contention",
            f"{cre['n_writers']} threads on one key, `max_retries={cre['max_retries_configured']}`, slow mutator",
            f"**{cre['n_that_hit_retry_exceeded']} failures, final value {cre['final_value']}/{cre['n_writers']}** -- writes lock instead of retrying",
        ])
    ttl = by_name.get("ttl_reschedule_no_resurrection")
    if ttl:
        rows.append([
            "TTL reschedule doesn't resurrect a key",
            f"Set `ttl=10`, immediately reschedule to `ttl=0.05`, repeat {ttl['trials']}x",
            f"**{ttl['unexpected_alive_after_short_ttl']} resurrections** -- stale long-deadline entry never fires late",
        ])
    sub = by_name.get("subscriber_exception_isolated")
    if sub:
        rows.append([
            "Subscriber exceptions don't break writes",
            "A subscriber that always throws, 500 writes in a row",
            f"**{'All 500 writes succeeded' if sub['passed'] else 'WRITES WERE LOST'}**, store integrity intact",
        ])

    lines.append(md_table(["Test", "What it actually does", "Result"], rows))
    lines.append("")
    lines.append(
        "**Finding worth keeping in mind:** `mset()`'s atomicity guarantee "
        "covers the *write* (one HAMT swap). It does **not** automatically "
        "give you a consistent multi-key *read* if you call `get()` twice "
        "-- for that you need `snapshot()`. This distinction isn't obvious "
        "from the README and is exactly the kind of thing breaking-it-on-"
        "purpose is supposed to surface.\n"
    )
    return "\n".join(lines)


def to_json(results):
    n_pass = sum(1 for r in results if r["passed"])
    return {
        "title": TITLE,
        "passed": n_pass,
        "total": len(results),
        "tests": [
            {
                "name": r["name"],
                "passed": r["passed"],
                "detail": {k: v for k, v in r.items() if k not in ("name", "passed")},
            }
            for r in results
        ],
        "keyFinding": (
            "mset()'s atomicity guarantee covers the write (one HAMT swap). "
            "It does not automatically give you a consistent multi-key read "
            "if you call get() twice -- for that you need snapshot()."
        ),
    }


if __name__ == "__main__":
    import json
    r = run()
    charts = render(r, None)
    print(to_markdown(r, charts))
    print(json.dumps(to_json(r), indent=2, default=str))
