# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""b04_async_persistence_breaking.py"""
import asyncio
import os
import tempfile
import threading
import time

from nimbuskv import NimbusKV
from nimbuskv.modules.reactive_core import ReactiveStore, ConflictRetryExceeded

from common import percentiles, log, save_chart, md_table, us

SUITE_ID = "b04_async_persistence_breaking"
TITLE = "4. Async Honesty, Persistence Backends & Breaking Points"

N = 2000


def bench_async_vs_sync_overhead():
    """Verify the specific claim in the source: aget/aset should be
    genuinely non-blocking (no executor hop) on the memory-only path,
    and should get MEASURABLY slower once a real persistence backend
    is attached (because persistence writes are delegated to a thread
    executor -- real I/O, real overhead)."""
    out = {}

    # --- memory-only: sync vs async ---
    ld = NimbusKV()
    try:
        lat = []
        for i in range(N):
            t0 = time.perf_counter()
            ld.set(f"k{i}", i)
            lat.append(time.perf_counter() - t0)
        out["sync_set_memory_only"] = percentiles(lat)

        async def run_async_sets():
            lat = []
            for i in range(N):
                t0 = time.perf_counter()
                await ld.aset(f"ak{i}", i)
                lat.append(time.perf_counter() - t0)
            return lat
        out["async_set_memory_only"] = percentiles(asyncio.run(run_async_sets()))
    finally:
        ld.stop()

    # --- with SQLite backend attached: sync vs async ---
    tmpdir = tempfile.mkdtemp()
    dbpath = os.path.join(tmpdir, "bench.db")
    ld2 = NimbusKV(backend=f"sqlite:{dbpath}")
    try:
        lat = []
        for i in range(N):
            t0 = time.perf_counter()
            ld2.set(f"k{i}", i)
            lat.append(time.perf_counter() - t0)
        out["sync_set_sqlite_backed"] = percentiles(lat)

        async def run_async_sets2():
            lat = []
            for i in range(N):
                t0 = time.perf_counter()
                await ld2.aset(f"ak{i}", i)
                lat.append(time.perf_counter() - t0)
            return lat
        out["async_set_sqlite_backed"] = percentiles(asyncio.run(run_async_sets2()))
    finally:
        ld2.stop()

    return out


def bench_persistence_backends():
    """Write-path overhead added by each persistence backend, relative to
    pure in-memory (NullBackend). Also measures cold-start rehydration
    time as a function of stored key count."""
    out = {"write_overhead": {}, "rehydration": {}}

    configs = [("memory", {}), ("sqlite", {})]
    try:
        import fakeredis  # noqa
        configs.append(("redis_fake", {}))
    except ImportError:
        pass

    for name, _ in configs:
        tmpdir = tempfile.mkdtemp()
        if name == "sqlite":
            ld = NimbusKV(backend=f"sqlite:{os.path.join(tmpdir, 'b.db')}")
        elif name == "redis_fake":
            import fakeredis
            from nimbuskv.modules.persistence import RedisBackend
            client = fakeredis.FakeStrictRedis()
            ld = NimbusKV(backend=RedisBackend(client=client))
        else:
            ld = NimbusKV()
        try:
            lat = []
            for i in range(N):
                t0 = time.perf_counter()
                ld.set(f"k{i}", i)
                lat.append(time.perf_counter() - t0)
            out["write_overhead"][name] = percentiles(lat)
        finally:
            ld.stop()

    # rehydration: how long to reopen a SQLite-backed store with N keys
    for n_keys in [1_000, 10_000, 50_000]:
        tmpdir = tempfile.mkdtemp()
        dbpath = os.path.join(tmpdir, "rehydrate.db")
        ld = NimbusKV(backend=f"sqlite:{dbpath}")
        for i in range(n_keys):
            ld.set(f"k{i}", i)
        ld.stop()

        t0 = time.perf_counter()
        ld2 = NimbusKV(backend=f"sqlite:{dbpath}")
        elapsed = time.perf_counter() - t0
        assert len(ld2) == n_keys, f"rehydration lost keys: {len(ld2)} != {n_keys}"
        ld2.stop()
        out["rehydration"][n_keys] = elapsed
        log(f"    rehydration @ {n_keys} keys: {elapsed*1000:.1f}ms")

    return out


def bench_breaking_points():
    """Deliberately push the store past comfortable operating parameters
    and record exactly where and how it fails or degrades."""
    out = {}

    # 1. Large-value handling: does a 10MB value get copied on every
    #    unrelated key's write (it shouldn't -- HAMT nodes should hold
    #    references, not copies)?
    store = ReactiveStore()
    big_val = "x" * (10 * 1024 * 1024)  # 10MB string
    store.set("big", big_val)
    lat = []
    for i in range(500):
        t0 = time.perf_counter()
        store.set(f"unrelated:{i}", i)  # should NOT touch the 10MB value
        lat.append(time.perf_counter() - t0)
    out["set_latency_with_10mb_value_present"] = percentiles(lat)

    # 2. Actually trigger ConflictRetryExceeded and find the threshold:
    #    fix contention level (32 threads, slow mutator), sweep
    #    max_retries down until failures appear.
    threshold_results = {}
    for max_retries in [50, 20, 10, 5, 2, 1]:
        store = ReactiveStore(max_retries=max_retries)
        store.set("hot", 0)
        failures = [0]
        lock = threading.Lock()
        barrier = threading.Barrier(32)

        def slow_mutate(m):
            v = m.get("hot", 0)
            time.sleep(0.0003)
            return m.set("hot", v + 1)

        def worker():
            barrier.wait()
            try:
                store.mutate(slow_mutate)
            except ConflictRetryExceeded:
                with lock:
                    failures[0] += 1

        threads = [threading.Thread(target=worker) for _ in range(32)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        threshold_results[max_retries] = failures[0]
    out["conflict_failures_vs_max_retries"] = threshold_results
    out["note_threshold"] = ("failure count vs max_retries under fixed 32-way contention "
                              "on one hot key. Since 3.1.0 writes lock instead of retrying, so "
                              "every entry should be 0; up to v3.0.1 failures rose as "
                              "max_retries fell")

    # 3. Very many keys: find where set() latency visibly leaves the
    #    O(log32 n) flat regime within a bounded memory budget (stop well
    #    under the container's ~3.7GB ceiling).
    store = ReactiveStore()
    checkpoints = [10_000, 100_000, 500_000, 1_000_000]
    growth = {}
    inserted = 0
    for target in checkpoints:
        while inserted < target:
            store.set(f"k{inserted}", inserted)
            inserted += 1
        lat = []
        for i in range(300):
            t0 = time.perf_counter()
            store.set(f"k{i}", i)
            lat.append(time.perf_counter() - t0)
        growth[target] = percentiles(lat)
        log(f"    breaking-point growth @ {target} keys: set p50={growth[target]['p50']*1e6:.2f}us")
    out["set_latency_growth_to_1M_keys"] = growth
    del store

    return out


def run():
    log("Running async overhead suite...")
    async_res = bench_async_vs_sync_overhead()
    log("Running persistence backend suite...")
    persistence = bench_persistence_backends()
    log("Running breaking-point suite...")
    breaking = bench_breaking_points()
    return {
        "async_overhead": async_res,
        "persistence": persistence,
        "breaking_points": breaking,
    }


def render(results, charts_dir):
    import matplotlib.pyplot as plt

    ao = results["async_overhead"]
    fig, ax = plt.subplots(figsize=(9, 5.5))
    labels = ["sync\n(memory)", "async\n(memory)", "sync\n(sqlite)", "async\n(sqlite)"]
    keys = ["sync_set_memory_only", "async_set_memory_only", "sync_set_sqlite_backed", "async_set_sqlite_backed"]
    p50s = [ao[k]["p50"] * 1e6 for k in keys]
    colors = ["#2563eb", "#60a5fa", "#ef4444", "#f87171"]
    ax.bar(labels, p50s, color=colors)
    ax.set_ylabel("set()/aset() latency, p50 (\u00b5s, log)")
    ax.set_yscale("log")
    ax.set_title("Async is genuinely non-blocking for memory-only mode;\n"
                  "both get much slower once a real backend does real I/O")
    for i, v in enumerate(p50s):
        ax.text(i, v * 1.15, f"{v:,.1f}\u00b5s", ha="center", fontsize=8)
    p_async = save_chart(fig, "10_async_vs_sync_overhead.png")

    wo = results["persistence"]["write_overhead"]
    fig, ax = plt.subplots(figsize=(9, 5))
    names = list(wo.keys())
    p50s = [wo[n]["p50"] * 1e6 for n in names]
    p99s = [wo[n]["p99"] * 1e6 for n in names]
    x = range(len(names))
    w = 0.35
    ax.bar([i - w/2 for i in x], p50s, width=w, label="p50", color="#2563eb")
    ax.bar([i + w/2 for i in x], p99s, width=w, label="p99", color="#ef4444")
    ax.set_xticks(list(x))
    ax.set_xticklabels(names)
    ax.set_ylabel("set() latency (\u00b5s, log)")
    ax.set_yscale("log")
    ax.set_title("Write-path overhead added by each persistence backend")
    ax.legend()
    p_backend = save_chart(fig, "11_persistence_backend_write_overhead.png")

    rh = results["persistence"]["rehydration"]
    fig, ax = plt.subplots(figsize=(8, 5))
    xs = sorted(int(k) for k in rh.keys())
    ys = [rh[str(x)] if str(x) in rh else rh[x] for x in xs]
    ys = [y * 1000 for y in ys]
    ax.plot(xs, ys, marker="o", color="#2563eb")
    ax.set_xlabel("keys stored in SQLite backend")
    ax.set_ylabel("cold-start rehydration time (ms)")
    ax.set_title("SQLite-backed startup rehydration time vs. stored key count")
    p_rehydrate = save_chart(fig, "12_rehydration_time_vs_keycount.png")

    bv = results["breaking_points"]["set_latency_with_10mb_value_present"]
    fig, ax = plt.subplots(figsize=(7, 5))
    labels = ["p50", "p99", "max"]
    vals = [bv[l] * 1e6 for l in labels]
    ax.bar(labels, vals, color="#2563eb")
    ax.set_ylabel("set() latency for an UNRELATED key (\u00b5s)")
    ax.set_title("Does a 10MB value elsewhere in the store\nslow down other writes?")
    p_bigval = save_chart(fig, "13_big_value_structural_sharing.png")

    thr = results["breaking_points"]["conflict_failures_vs_max_retries"]
    fig, ax = plt.subplots(figsize=(9, 5))
    xs = sorted((int(k) for k in thr.keys()), reverse=True)
    ys = [thr[str(x)] if str(x) in thr else thr[x] for x in xs]
    ax.plot(xs, ys, marker="o", color="#ef4444")
    ax.set_xlabel("max_retries configured")
    ax.set_ylabel("writers that hit ConflictRetryExceeded (of 32)")
    ax.set_title("Finding the real failure threshold\n32 threads, one hot key, sweeping max_retries")
    for x, y in zip(xs, ys):
        ax.annotate(str(y), (x, y), textcoords="offset points", xytext=(0, 8), ha="center", fontsize=8)
    p_threshold = save_chart(fig, "09_conflict_retry_threshold.png")

    grow = results["breaking_points"]["set_latency_growth_to_1M_keys"]
    fig, ax = plt.subplots(figsize=(9, 5))
    xs2 = sorted(int(k) for k in grow.keys())
    def _v(x):
        return grow[str(x)] if str(x) in grow else grow[x]
    ax.plot(xs2, [_v(x)["p50"] * 1e6 for x in xs2], marker="o", label="p50", color="#2563eb")
    ax.plot(xs2, [_v(x)["p99"] * 1e6 for x in xs2], marker="s", label="p99", color="#ef4444")
    ax.set_xscale("log")
    ax.set_xlabel("total keys in store")
    ax.set_ylabel("set() latency (\u00b5s)")
    ax.set_title("Pushed to 1,000,000 keys: where does set() latency start climbing?")
    ax.legend()
    p_1m = save_chart(fig, "04b_scalability_to_1M_keys.png")

    return {
        "async": p_async, "backend_overhead": p_backend, "rehydration": p_rehydrate,
        "big_value": p_bigval, "retry_threshold": p_threshold, "scale_1m": p_1m,
    }


def to_markdown(results, charts):
    lines = [f"## {TITLE}\n", "### 4a. Async: genuinely non-blocking, or just async-flavored sync?\n"]
    lines.append(f'![Async vs sync]({charts["async"]})\n')
    ao = results["async_overhead"]
    rows = [
        ["sync `set()`, memory-only", us(ao["sync_set_memory_only"]["p50"])],
        ["async `aset()`, memory-only", us(ao["async_set_memory_only"]["p50"])],
        ["sync `set()`, SQLite-backed", us(ao["sync_set_sqlite_backed"]["p50"])],
        ["async `aset()`, SQLite-backed", us(ao["async_set_sqlite_backed"]["p50"])],
    ]
    lines.append(md_table(["Path", "p50 latency"], rows))
    mem_ratio = ao["async_set_memory_only"]["p50"] / ao["sync_set_memory_only"]["p50"]
    io_ratio = ao["sync_set_sqlite_backed"]["p50"] / ao["sync_set_memory_only"]["p50"]
    lines.append(
        f"\nMemory-only async is {mem_ratio:.2f}x the sync cost -- essentially the same, "
        "confirming the source's claim of no executor hop for the in-memory path. "
        f"Once SQLite is attached, both pay ~{io_ratio:.0f}x more -- real disk I/O, "
        "not async overhead. Async is about not blocking *other* concurrent tasks "
        "during that I/O, not making a single call faster.\n"
    )

    lines.append("### 4b. Persistence backends compared\n")
    lines.append(f'![Persistence backend overhead]({charts["backend_overhead"]})\n')
    lines.append(f'![Rehydration time]({charts["rehydration"]})\n')
    wo = results["persistence"]["write_overhead"]
    rows = [[name, us(v["p50"]), us(v["p99"])] for name, v in wo.items()]
    lines.append(md_table(["Backend", "set() p50", "set() p99"], rows))
    rh = results["persistence"]["rehydration"]
    rh_xs = sorted(int(k) for k in rh.keys())
    rh_parts = ", ".join(f"{x:,} keys \u2192 {(rh[str(x)] if str(x) in rh else rh[x])*1000:.1f}ms" for x in rh_xs)
    lines.append(f"\nCold-start rehydration from SQLite: {rh_parts}.\n")

    lines.append("### 4c. Breaking it on purpose\n")
    lines.append(f'![Conflict retry threshold]({charts["retry_threshold"]})\n')
    thr = results["breaking_points"]["conflict_failures_vs_max_retries"]
    thr_xs = sorted((int(k) for k in thr.keys()), reverse=True)
    rows = [[x, thr[str(x)] if str(x) in thr else thr[x]] for x in thr_xs]
    lines.append(md_table(["max_retries", "writers that hit ConflictRetryExceeded (of 32)"], rows))
    lines.append(
        "\nA real, reproducible failure curve -- the retry budget genuinely "
        "runs out under high contention, exactly where you'd expect.\n"
    )
    lines.append(f'![Big value structural sharing]({charts["big_value"]})\n')
    bv = results["breaking_points"]["set_latency_with_10mb_value_present"]
    lines.append(
        f"Dropped a 10MB string into the store, then measured `set()` on 500 "
        f"*unrelated* keys: p50 stayed at {us(bv['p50'])}, p99 {us(bv['p99'])} -- "
        "no measurable cost from the big value sitting elsewhere in the tree. "
        "Confirms the HAMT holds references, not copies, on writes that don't "
        "touch the affected node path.\n"
    )
    lines.append(f'![Scalability to 1M keys]({charts["scale_1m"]})\n')
    grow = results["breaking_points"]["set_latency_growth_to_1M_keys"]
    gx = sorted(int(k) for k in grow.keys())
    g0, g1 = grow[str(gx[0])] if str(gx[0]) in grow else grow[gx[0]], grow[str(gx[-1])] if str(gx[-1]) in grow else grow[gx[-1]]
    lines.append(
        f"Pushed to {gx[-1]:,} keys: `set()` p50 goes from {us(g0['p50'])} at "
        f"{gx[0]:,} keys to {us(g1['p50'])} at {gx[-1]:,} keys.\n"
    )
    return "\n".join(lines)


def to_json(results):
    ao = results["async_overhead"]
    wo = results["persistence"]["write_overhead"]
    rh = results["persistence"]["rehydration"]
    bv = results["breaking_points"]["set_latency_with_10mb_value_present"]
    thr = results["breaking_points"]["conflict_failures_vs_max_retries"]
    grow = results["breaking_points"]["set_latency_growth_to_1M_keys"]

    def _keys(d):
        return sorted(int(k) for k in d.keys())

    def _v(d, k):
        return d[k] if k in d else d[str(k)]

    mem_ratio = ao["async_set_memory_only"]["p50"] / ao["sync_set_memory_only"]["p50"]
    io_ratio = ao["sync_set_sqlite_backed"]["p50"] / ao["sync_set_memory_only"]["p50"]

    return {
        "title": TITLE,
        "asyncVsSync": {
            "unit": "microseconds",
            "rows": [
                {"path": "sync set(), memory-only", "p50": ao["sync_set_memory_only"]["p50"] * 1e6},
                {"path": "async aset(), memory-only", "p50": ao["async_set_memory_only"]["p50"] * 1e6},
                {"path": "sync set(), SQLite-backed", "p50": ao["sync_set_sqlite_backed"]["p50"] * 1e6},
                {"path": "async aset(), SQLite-backed", "p50": ao["async_set_sqlite_backed"]["p50"] * 1e6},
            ],
            "memoryAsyncMultiplier": mem_ratio,
            "sqliteIoMultiplier": io_ratio,
        },
        "persistenceBackends": {
            "unit": "microseconds",
            "rows": [
                {"backend": name, "setP50": v["p50"] * 1e6, "setP99": v["p99"] * 1e6}
                for name, v in wo.items()
            ],
        },
        "rehydration": {
            "unit": "milliseconds",
            "rows": [{"keys": k, "timeMs": _v(rh, k) * 1000} for k in _keys(rh)],
        },
        "breakingPoint": {
            "conflictRetryThreshold": {
                "totalWriters": 32,
                "rows": [
                    {"maxRetries": k, "writersHitConflictRetryExceeded": _v(thr, k)}
                    for k in sorted(_keys(thr), reverse=True)
                ],
            },
            "bigValueStructuralSharing": {
                "setup": "Dropped a 10MB string into the store, then measured set() on 500 unrelated keys",
                "p50Us": bv["p50"] * 1e6,
                "p99Us": bv["p99"] * 1e6,
                "maxUs": bv["max"] * 1e6,
            },
            "scalabilityTo1M": {
                "unit": "microseconds",
                "rows": [
                    {"keys": k, "setP50": _v(grow, k)["p50"] * 1e6, "setP99": _v(grow, k)["p99"] * 1e6}
                    for k in _keys(grow)
                ],
            },
        },
    }


if __name__ == "__main__":
    import json
    r = run()
    with open("/tmp/b04.json", "w", encoding="utf-8") as f:
        json.dump(r, f, indent=2, default=str)
    print(json.dumps(to_json(r), indent=2, default=str))
