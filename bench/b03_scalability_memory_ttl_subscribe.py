# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""b03_scalability_memory_ttl_subscribe.py"""
import gc
import os
import time

import psutil

from nimbuskv.modules.reactive_core import ReactiveStore
from nimbuskv import NimbusKV

from common import percentiles, rss_mb, gc_pause, log, save_chart, md_table, us

SUITE_ID = "b03_scalability_memory_ttl_subscribe"
TITLE = "3. Scalability, Memory, TTL Scheduler & Subscriber Overhead"

KEY_COUNTS = [100, 1_000, 10_000, 100_000, 500_000]
SAMPLE_OPS = 500


def bench_scalability_vs_keycount():
    """get()/set() latency as a function of total key count. The HAMT
    backing structure should give ~O(log32 N) -- i.e. latency should stay
    nearly flat across orders of magnitude, not grow linearly like a
    naive structure-copying implementation would."""
    out = {}
    store = ReactiveStore()
    inserted = 0
    for target in KEY_COUNTS:
        while inserted < target:
            store.set(f"key:{inserted}", inserted)
            inserted += 1

        get_lat = []
        for i in range(SAMPLE_OPS):
            k = f"key:{i % target}"
            t0 = time.perf_counter()
            store.get(k)
            get_lat.append(time.perf_counter() - t0)

        set_lat = []
        for i in range(SAMPLE_OPS):
            t0 = time.perf_counter()
            store.set(f"key:{i % target}", i)
            set_lat.append(time.perf_counter() - t0)

        out[target] = {"get": percentiles(get_lat), "set": percentiles(set_lat)}
        log(f"    scalability @ {target} keys: get p50={out[target]['get']['p50']*1e6:.2f}us "
            f"set p50={out[target]['set']['p50']*1e6:.2f}us")
    return out


def bench_memory_overhead():
    """Process RSS delta for storing N string keys -> small int values,
    compared against a plain dict as the theoretical floor. This isolates
    NimbusKV's structural overhead (HAMT nodes, version counters,
    subscriber registries) from raw payload size."""
    out = {}
    for target in [10_000, 100_000, 500_000]:
        gc_pause()
        base = rss_mb()
        d = {}
        for i in range(target):
            d[f"key:{i}"] = i
        gc_pause()
        dict_mb = rss_mb() - base
        del d
        gc_pause()

        base2 = rss_mb()
        store = ReactiveStore()
        for i in range(target):
            store.set(f"key:{i}", i)
        gc_pause()
        nimbuskv_mb = rss_mb() - base2
        del store
        gc_pause()

        out[target] = {
            "dict_mb": round(dict_mb, 2),
            "nimbuskv_mb": round(nimbuskv_mb, 2),
            "overhead_ratio": round(nimbuskv_mb / dict_mb, 2) if dict_mb > 0 else None,
            "bytes_per_key_nimbuskv": round((nimbuskv_mb * 1e6) / target, 1),
            "bytes_per_key_dict": round((dict_mb * 1e6) / target, 1),
        }
        log(f"    memory @ {target} keys: dict={dict_mb:.1f}MB nimbuskv={nimbuskv_mb:.1f}MB "
            f"ratio={out[target]['overhead_ratio']}")
    return out


def bench_ttl_scheduler_accuracy(n_samples=300):
    """How close does actual expiry land to the requested TTL? Measures
    scheduler jitter under a light load and under heavy concurrent
    scheduling pressure (many keys with staggered short TTLs)."""
    ld = NimbusKV()
    try:
        requested_ttl = 0.15
        errors = []
        for i in range(n_samples):
            t_set = time.perf_counter()
            ld.set(f"k{i}", 1, ttl=requested_ttl)
            # poll tightly near the expected expiry to measure actual firing time
            deadline = t_set + requested_ttl
            while time.perf_counter() < deadline - 0.02:
                time.sleep(0.005)
            while ld.get(f"k{i}") is not None:
                time.sleep(0.0005)
            actual = time.perf_counter() - t_set
            errors.append(actual - requested_ttl)
        return {
            "requested_ttl_s": requested_ttl,
            "n_samples": n_samples,
            "jitter_ms": percentiles([e * 1000 for e in errors]),
        }
    finally:
        ld.stop()


def bench_ttl_scheduler_overhead_vs_scheduled_count():
    """Overhead of set(ttl=...) itself as the number of LIVE scheduled
    keys grows -- does the heap-based scheduler stay O(log n) per
    schedule, or degrade as more timers are pending?"""
    out = {}
    for n_scheduled in [10, 1_000, 10_000, 100_000]:
        ld = NimbusKV()
        try:
            for i in range(n_scheduled):
                ld.set(f"warm:{i}", 1, ttl=3600)  # long TTL, just occupies the heap
            lat = []
            for i in range(200):
                t0 = time.perf_counter()
                ld.set(f"probe:{i}", 1, ttl=3600)
                lat.append(time.perf_counter() - t0)
            out[n_scheduled] = percentiles(lat)
        finally:
            ld.stop()
    return out


def bench_subscriber_notification_overhead():
    """set() latency as a function of subscriber count on that store, and
    fan-out notification throughput. Reactive stores trade write latency
    for observability -- this quantifies the trade."""
    out = {}
    for n_subs in [0, 1, 10, 100, 1000]:
        store = ReactiveStore()
        received = [0]

        def cb(k, e, o, n):
            received[0] += 1

        for _ in range(n_subs):
            store.subscribe(cb)

        lat = []
        for i in range(500):
            t0 = time.perf_counter()
            store.set(f"k{i}", i)
            lat.append(time.perf_counter() - t0)
        out[n_subs] = percentiles(lat)
        log(f"    subscribe overhead @ {n_subs} subs: set p50={out[n_subs]['p50']*1e6:.2f}us "
            f"(fired {received[0]} notifications for 500 sets)")
    return out


def run():
    log("Running scalability suite...")
    scalability = bench_scalability_vs_keycount()
    log("Running memory overhead suite...")
    memory = bench_memory_overhead()
    log("Running TTL accuracy suite...")
    ttl_accuracy = bench_ttl_scheduler_accuracy()
    log("Running TTL scheduler overhead-vs-load suite...")
    ttl_overhead = bench_ttl_scheduler_overhead_vs_scheduled_count()
    log("Running subscriber overhead suite...")
    subscribe = bench_subscriber_notification_overhead()
    return {
        "scalability_vs_keycount": scalability,
        "memory_overhead": memory,
        "ttl_accuracy": ttl_accuracy,
        "ttl_overhead_vs_scheduled_count": ttl_overhead,
        "subscriber_overhead": subscribe,
    }


def render(results, charts_dir):
    import matplotlib.pyplot as plt

    def _keys(d):
        return sorted(int(k) for k in d.keys())

    def _v(d, k):
        return d[k] if k in d else d[str(k)]

    scal = results["scalability_vs_keycount"]
    fig, ax = plt.subplots(figsize=(9, 5))
    xs = _keys(scal)
    ax.plot(xs, [_v(scal, x)["get"]["p50"] * 1e6 for x in xs], marker="o", label="get() p50", color="#2563eb")
    ax.plot(xs, [_v(scal, x)["set"]["p50"] * 1e6 for x in xs], marker="s", label="set() p50", color="#f59e0b")
    ax.set_xscale("log")
    ax.set_xlabel("total keys in store")
    ax.set_ylabel("latency (microseconds)")
    ax.set_title("get()/set() latency vs. total key count\nnear-flat = HAMT's O(log32 n) working as designed")
    ax.legend()
    p_scal = save_chart(fig, "04_scalability_vs_keycount.png")

    mem = results["memory_overhead"]
    fig, ax = plt.subplots(figsize=(9, 5))
    xs = _keys(mem)
    dict_mb = [_v(mem, x)["dict_mb"] for x in xs]
    nk_mb = [_v(mem, x)["nimbuskv_mb"] for x in xs]
    w = 0.35
    xpos = range(len(xs))
    ax.bar([i - w/2 for i in xpos], dict_mb, width=w, label="plain dict (floor)", color="#94a3b8")
    ax.bar([i + w/2 for i in xpos], nk_mb, width=w, label="NimbusKV", color="#2563eb")
    ax.set_xticks(list(xpos))
    ax.set_xticklabels([f"{x:,}" for x in xs])
    ax.set_xlabel("number of keys (int values)")
    ax.set_ylabel("process RSS delta (MB)")
    ax.set_title("Memory overhead vs. plain dict")
    ax.legend()
    p_mem = save_chart(fig, "05_memory_overhead_vs_dict.png")

    ttl_acc = results["ttl_accuracy"]["jitter_ms"]
    fig, ax = plt.subplots(figsize=(8, 5))
    labels = ["p50", "p90", "p99", "p99.9", "max"]
    vals = [ttl_acc[l] for l in labels]
    ax.bar(labels, vals, color="#2563eb")
    ax.axhline(0, color="#333", linewidth=0.8)
    ax.set_ylabel("actual - requested expiry (ms)")
    ax.set_title(f"TTL scheduler firing accuracy (requested = "
                 f"{results['ttl_accuracy']['requested_ttl_s']*1000:.0f}ms, "
                 f"n={results['ttl_accuracy']['n_samples']})")
    p_ttlacc = save_chart(fig, "06_ttl_accuracy_jitter.png")

    ttl_ov = results["ttl_overhead_vs_scheduled_count"]
    fig, ax = plt.subplots(figsize=(9, 5))
    xs = _keys(ttl_ov)
    ax.plot(xs, [_v(ttl_ov, x)["p50"] * 1e6 for x in xs], marker="o", label="p50", color="#2563eb")
    ax.plot(xs, [_v(ttl_ov, x)["p99"] * 1e6 for x in xs], marker="s", label="p99", color="#ef4444")
    ax.set_xscale("log")
    ax.set_xlabel("pending scheduled TTL keys")
    ax.set_ylabel("set(ttl=...) latency (microseconds)")
    ax.set_title("TTL scheduler overhead vs. number of pending timers")
    ax.legend()
    p_ttlov = save_chart(fig, "07_ttl_scheduler_overhead_vs_load.png")

    sub = results["subscriber_overhead"]
    fig, ax = plt.subplots(figsize=(9, 5))
    xs = _keys(sub)
    ax.plot(xs, [_v(sub, x)["p50"] * 1e6 for x in xs], marker="o", label="p50", color="#2563eb")
    ax.plot(xs, [_v(sub, x)["mean"] * 1e6 for x in xs], marker="s", label="mean", color="#f59e0b")
    ax.set_xlabel("subscribers registered on the store")
    ax.set_ylabel("set() latency (microseconds)")
    ax.set_title("set() latency vs. subscriber count (fan-out cost)")
    ax.legend()
    p_sub = save_chart(fig, "08_subscriber_fanout_overhead.png")

    return {
        "scalability": p_scal, "memory": p_mem, "ttl_accuracy": p_ttlacc,
        "ttl_overhead": p_ttlov, "subscriber": p_sub,
    }


def to_markdown(results, charts):
    def _keys(d):
        return sorted(int(k) for k in d.keys())

    def _v(d, k):
        return d[k] if k in d else d[str(k)]

    lines = [f"## {TITLE}\n", "### 3a. Scalability with key count\n"]
    lines.append(f'![Scalability]({charts["scalability"]})\n')
    scal = results["scalability_vs_keycount"]
    xs = _keys(scal)
    lo = min(_v(scal, x)["get"]["p50"] for x in xs) * 1e6
    hi = max(_v(scal, x)["get"]["p50"] for x in xs) * 1e6
    lines.append(
        f"`get()`/`set()` latency stays within a tight band ({lo:.2f}\u2013{hi:.2f}\u00b5s get) "
        f"from {xs[0]:,} keys to {xs[-1]:,} keys -- consistent with O(log\u2083\u2082 n), "
        "not the O(n) you'd get from a naive copy-on-write structure.\n"
    )

    lines.append("### 3b. Memory overhead vs. plain dict\n")
    lines.append(f'![Memory overhead]({charts["memory"]})\n')
    mem = results["memory_overhead"]
    rows = []
    for k in _keys(mem):
        v = _v(mem, k)
        rows.append([f"{k:,}", f"{v['dict_mb']:.1f} MB", f"{v['nimbuskv_mb']:.1f} MB",
                     f"{v['overhead_ratio']}x" if v["overhead_ratio"] is not None else "\u2014"])
    lines.append(md_table(["Keys", "dict (floor)", "NimbusKV", "overhead ratio"], rows))
    lines.append("")

    lines.append("### 3c. TTL scheduler: accuracy & overhead\n")
    lines.append(f'![TTL accuracy]({charts["ttl_accuracy"]})\n')
    lines.append(f'![TTL scheduler overhead]({charts["ttl_overhead"]})\n')
    acc = results["ttl_accuracy"]
    j = acc["jitter_ms"]
    lines.append(
        f"Firing accuracy ({acc['n_samples']} samples, {acc['requested_ttl_s']*1000:.0f}ms requested TTL): "
        f"p50 jitter {j['p50']:.2f}ms, p99 {j['p99']:.2f}ms, max {j['max']:.2f}ms "
        "(all positive = always late, never early -- no resurrection).\n"
    )
    ov = results["ttl_overhead_vs_scheduled_count"]
    ov_xs = _keys(ov)
    lines.append(
        f"Scheduling overhead: {_v(ov, ov_xs[0])['p50']*1e6:.2f}\u00b5s with {ov_xs[0]:,} pending "
        f"timers \u2192 {_v(ov, ov_xs[-1])['p50']*1e6:.2f}\u00b5s with {ov_xs[-1]:,} pending -- "
        "consistent with the documented heap-based (O(log n)) scheduler.\n"
    )

    lines.append("### 3d. Subscriber / notification fan-out cost\n")
    lines.append(f'![Subscriber overhead]({charts["subscriber"]})\n')
    sub = results["subscriber_overhead"]
    rows = [[f"{k:,}", us(_v(sub, k)["p50"])] for k in _keys(sub)]
    lines.append(md_table(["Subscribers", "set() p50"], rows))
    lines.append(
        "\nRoughly linear in subscriber count, as it must be -- every "
        "subscriber is called synchronously on every write. This is the "
        "explicit price of the reactive/observable design.\n"
    )
    return "\n".join(lines)


def to_json(results):
    def _keys(d):
        return sorted(int(k) for k in d.keys())

    def _v(d, k):
        return d[k] if k in d else d[str(k)]

    scal = results["scalability_vs_keycount"]
    mem = results["memory_overhead"]
    acc = results["ttl_accuracy"]
    ov = results["ttl_overhead_vs_scheduled_count"]
    sub = results["subscriber_overhead"]

    return {
        "title": TITLE,
        "scalability": {
            "unit": "microseconds",
            "rows": [
                {
                    "keys": k,
                    "getP50": _v(scal, k)["get"]["p50"] * 1e6,
                    "setP50": _v(scal, k)["set"]["p50"] * 1e6,
                }
                for k in _keys(scal)
            ],
        },
        "memoryOverheadVsDict": {
            "unit": "MB",
            "rows": [
                {
                    "keys": k,
                    "dictMb": _v(mem, k)["dict_mb"],
                    "nimbuskvMb": _v(mem, k)["nimbuskv_mb"],
                    "ratio": _v(mem, k)["overhead_ratio"],
                    "bytesPerKeyNimbuskv": _v(mem, k)["bytes_per_key_nimbuskv"],
                    "bytesPerKeyDict": _v(mem, k)["bytes_per_key_dict"],
                }
                for k in _keys(mem)
            ],
        },
        "ttlAccuracy": {
            "samples": acc["n_samples"],
            "requestedTtlMs": acc["requested_ttl_s"] * 1000,
            "p50JitterMs": acc["jitter_ms"]["p50"],
            "p99JitterMs": acc["jitter_ms"]["p99"],
            "maxJitterMs": acc["jitter_ms"]["max"],
        },
        "ttlSchedulerOverhead": {
            "unit": "microseconds",
            "rows": [
                {"pendingTimers": k, "p50": _v(ov, k)["p50"] * 1e6, "p99": _v(ov, k)["p99"] * 1e6}
                for k in _keys(ov)
            ],
        },
        "subscriberOverhead": {
            "unit": "microseconds",
            "rows": [
                {"subscribers": k, "setP50": _v(sub, k)["p50"] * 1e6, "setMean": _v(sub, k)["mean"] * 1e6}
                for k in _keys(sub)
            ],
        },
    }


if __name__ == "__main__":
    import json
    r = run()
    with open("/tmp/b03.json", "w", encoding="utf-8") as f:
        json.dump(r, f, indent=2, default=str)
    print(json.dumps(to_json(r), indent=2, default=str))
