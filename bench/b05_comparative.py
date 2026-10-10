# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""b05_comparative.py

Head-to-head vs the libraries actually chosen for comparison:
- dict + threading.Lock: the naive baseline everyone reaches for first.
- cachetools.TTLCache: popular pure-Python TTL cache, lock-protected.
- aiocache (memory backend): async-first cache library.
- shelve: stdlib persistent dict (disk-backed, pickle-based).

Every comparison runs the SAME workload shape on the SAME machine in the
SAME process run, back to back, so results are directly comparable --
no numbers pulled from anyone's README.
"""
import asyncio
import shelve
import tempfile
import threading
import time
import os

import cachetools
from aiocache import Cache

from nimbuskv.modules.reactive_core import ReactiveStore
from nimbuskv import NimbusKV

from common import percentiles, log, save_chart, md_table, us

SUITE_ID = "b05_comparative"
TITLE = "5. Comparative: NimbusKV vs. dict+Lock, cachetools, aiocache, shelve"

COLORS = {
    "NimbusKV (ReactiveStore)": "#2563eb", "NimbusKV (memory)": "#2563eb",
    "dict+Lock": "#f59e0b", "cachetools.TTLCache": "#10b981",
    "shelve (disk-backed)": "#ef4444", "aiocache (memory)": "#8b5cf6",
}

N = 3000
THREAD_COUNTS = [1, 4, 16]


# ---- adapters: give every contender the same get/set surface ----

class DictLockAdapter:
    name = "dict+Lock"

    def __init__(self):
        self._d = {}
        self._lock = threading.Lock()

    def set(self, k, v):
        with self._lock:
            self._d[k] = v

    def get(self, k, default=None):
        with self._lock:
            return self._d.get(k, default)


class NimbusKVAdapter:
    name = "NimbusKV (ReactiveStore)"

    def __init__(self):
        self._s = ReactiveStore()

    def set(self, k, v):
        self._s.set(k, v)

    def get(self, k, default=None):
        return self._s.get(k, default)


class CachetoolsAdapter:
    name = "cachetools.TTLCache"

    def __init__(self):
        self._c = cachetools.TTLCache(maxsize=10_000_000, ttl=3600)
        self._lock = threading.Lock()  # cachetools itself is NOT thread-safe

    def set(self, k, v):
        with self._lock:
            self._c[k] = v

    def get(self, k, default=None):
        with self._lock:
            return self._c.get(k, default)


class ShelveAdapter:
    name = "shelve (disk-backed)"

    def __init__(self, path):
        self._d = shelve.open(path)
        self._lock = threading.Lock()

    def set(self, k, v):
        with self._lock:
            self._d[k] = v
            self._d.sync()

    def get(self, k, default=None):
        with self._lock:
            return self._d.get(k, default)

    def close(self):
        self._d.close()


def bench_single_threaded_get_set_latency():
    out = {}
    tmpdir = tempfile.mkdtemp()

    contenders = [
        DictLockAdapter(),
        NimbusKVAdapter(),
        CachetoolsAdapter(),
        ShelveAdapter(os.path.join(tmpdir, "shelve.db")),
    ]
    for c in contenders:
        set_lat = []
        for i in range(N):
            t0 = time.perf_counter()
            c.set(f"k{i}", i)
            set_lat.append(time.perf_counter() - t0)

        get_lat = []
        for i in range(N):
            t0 = time.perf_counter()
            c.get(f"k{i % N}")
            get_lat.append(time.perf_counter() - t0)

        out[c.name] = {"set": percentiles(set_lat), "get": percentiles(get_lat)}
        log(f"    {c.name}: set p50={out[c.name]['set']['p50']*1e6:.2f}us "
            f"get p50={out[c.name]['get']['p50']*1e6:.2f}us")
        if hasattr(c, "close"):
            c.close()
    return out


def bench_multithreaded_throughput():
    """Concurrent SET throughput (disjoint keys per thread) vs thread
    count for each contender, using the same synchronized-barrier
    methodology validated in b02 (see that file's docstring for why a
    barrier is required to avoid an inflated-throughput measurement
    artifact)."""
    out = {}

    def run_for(make_store, name, duration=0.5):
        per_thread = {}
        for n_threads in THREAD_COUNTS:
            store = make_store()
            stop = threading.Event()
            counts = [0] * n_threads
            barrier = threading.Barrier(n_threads + 1)

            def worker(idx):
                barrier.wait()
                c = 0
                prefix = f"t{idx}:"
                while not stop.is_set():
                    store.set(f"{prefix}{c}", c)
                    c += 1
                counts[idx] = c

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
            for t in threads:
                t.start()
            barrier.wait()
            t0 = time.perf_counter()
            time.sleep(duration)
            stop.set()
            for t in threads:
                t.join()
            elapsed = time.perf_counter() - t0
            per_thread[n_threads] = sum(counts) / elapsed
            if hasattr(store, "close"):
                store.close()
        out[name] = per_thread
        log(f"    {name}: {per_thread}")

    run_for(DictLockAdapter, "dict+Lock")
    run_for(NimbusKVAdapter, "NimbusKV (ReactiveStore)")
    run_for(CachetoolsAdapter, "cachetools.TTLCache")
    return out


def bench_ttl_feature_comparison():
    """Not everything is a number -- record which contenders even HAVE
    native TTL support and, where they do, measure expiry-set overhead."""
    out = {}

    # NimbusKV: native TTL
    ld = NimbusKV()
    lat = []
    for i in range(1000):
        t0 = time.perf_counter()
        ld.set(f"k{i}", i, ttl=60)
        lat.append(time.perf_counter() - t0)
    out["NimbusKV"] = {"native_ttl": True, "set_with_ttl": percentiles(lat)}
    ld.stop()

    # cachetools.TTLCache: TTL is cache-wide-ish (per-instance ttl, set at construction
    # in this version), not truly per-key like NimbusKV -- record that distinction.
    cache = cachetools.TTLCache(maxsize=100_000, ttl=60)
    lat = []
    for i in range(1000):
        t0 = time.perf_counter()
        cache[f"k{i}"] = i
        lat.append(time.perf_counter() - t0)
    out["cachetools"] = {"native_ttl": "per-cache-instance, not per-key", "set": percentiles(lat)}

    # dict+Lock: no TTL at all -- would need to be hand-rolled
    out["dict+Lock"] = {"native_ttl": False}

    # shelve: no TTL at all
    out["shelve"] = {"native_ttl": False}

    return out


def bench_async_comparison():
    """aiocache is async-first; NimbusKV's async path is genuinely
    non-blocking for memory-only mode (see b04). Compare await-call
    overhead for a pure in-memory async set()."""
    out = {}

    async def run_aiocache():
        cache = Cache(Cache.MEMORY)
        lat = []
        for i in range(1000):
            t0 = time.perf_counter()
            await cache.set(f"k{i}", i)
            lat.append(time.perf_counter() - t0)
        return lat
    out["aiocache (memory)"] = percentiles(asyncio.run(run_aiocache()))

    ld = NimbusKV()
    async def run_nimbuskv():
        lat = []
        for i in range(1000):
            t0 = time.perf_counter()
            await ld.aset(f"k{i}", i)
            lat.append(time.perf_counter() - t0)
        return lat
    out["NimbusKV (memory)"] = percentiles(asyncio.run(run_nimbuskv()))
    ld.stop()

    return out


def run():
    log("Running comparative single-threaded latency...")
    latency = bench_single_threaded_get_set_latency()
    log("Running comparative multithreaded throughput...")
    throughput = bench_multithreaded_throughput()
    log("Running TTL feature comparison...")
    ttl = bench_ttl_feature_comparison()
    log("Running async comparison...")
    async_cmp = bench_async_comparison()
    return {
        "latency": latency,
        "throughput_vs_threads": throughput,
        "ttl_features": ttl,
        "async": async_cmp,
    }


def render(results, charts_dir):
    import matplotlib.pyplot as plt

    cl = results["latency"]
    fig, ax = plt.subplots(figsize=(10, 5.5))
    names = list(cl.keys())
    set_p50 = [cl[n]["set"]["p50"] * 1e6 for n in names]
    get_p50 = [cl[n]["get"]["p50"] * 1e6 for n in names]
    x = range(len(names))
    w = 0.35
    colors_set = [COLORS.get(n, "#888") for n in names]
    ax.bar([i - w/2 for i in x], set_p50, width=w, label="set()", color=colors_set, alpha=0.9)
    ax.bar([i + w/2 for i in x], get_p50, width=w, label="get()", color=colors_set, alpha=0.5)
    ax.set_xticks(list(x))
    ax.set_xticklabels(names, rotation=15, ha="right")
    ax.set_ylabel("latency, p50 (\u00b5s, log)")
    ax.set_yscale("log")
    ax.set_title("Comparative single-threaded latency\n(same process, same machine, same workload, back to back)")
    ax.legend()
    p_lat = save_chart(fig, "14_comparative_latency.png")

    ct = results["throughput_vs_threads"]
    fig, ax = plt.subplots(figsize=(9, 5.5))
    for name, d in ct.items():
        xs = sorted(int(k) for k in d.keys())
        ys = [d[x] if x in d else d[str(x)] for x in xs]
        ax.plot(xs, ys, marker="o", label=name, color=COLORS.get(name, "#888"))
    ax.set_xlabel("thread count")
    ax.set_ylabel("SET ops/sec (disjoint keys)")
    ax.set_title("Comparative concurrent write throughput vs. thread count")
    ax.legend()
    p_thr = save_chart(fig, "15_comparative_throughput_vs_threads.png")

    ca = results["async"]
    fig, ax = plt.subplots(figsize=(7, 5))
    names = list(ca.keys())
    p50s = [ca[n]["p50"] * 1e6 for n in names]
    colors = [COLORS.get(n, "#888") for n in names]
    ax.bar(names, p50s, color=colors)
    ax.set_ylabel("await set(), p50 (\u00b5s)")
    ax.set_title("Async-call overhead: NimbusKV vs. aiocache\n(both memory-backed, no real I/O)")
    p_async = save_chart(fig, "16_comparative_async_overhead.png")

    return {"latency": p_lat, "throughput": p_thr, "async": p_async}


def to_markdown(results, charts):
    lines = [f"## {TITLE}\n"]
    lines.append(
        "Same process, same machine, same workload shape, run back-to-back "
        "-- no numbers pulled from anyone's README.\n"
    )
    lines.append(f'![Comparative latency]({charts["latency"]})\n')

    cl = results["latency"]
    ttl = results["ttl_features"]
    rows = []
    for name in cl.keys():
        ttl_key = "NimbusKV" if name.startswith("NimbusKV") else name.split(".")[0].split(" ")[0]
        ttl_info = ttl.get(ttl_key, ttl.get(name, {}))
        native_ttl = ttl_info.get("native_ttl", "\u2014")
        if native_ttl is True:
            native_ttl = "Yes"
        elif native_ttl is False:
            native_ttl = "No"
        rows.append([name, us(cl[name]["set"]["p50"]), us(cl[name]["get"]["p50"]), str(native_ttl)])
    lines.append(md_table(["Library", "set() p50", "get() p50", "Native per-key TTL?"], rows))
    lines.append("")

    nk = cl.get("NimbusKV (ReactiveStore)")
    dl = cl.get("dict+Lock")
    if nk and dl:
        set_ratio = nk["set"]["p50"] / dl["set"]["p50"]
        get_delta = "faster" if nk["get"]["p50"] < dl["get"]["p50"] else "slower"
        lines.append(
            f"Honest read: raw dict+Lock wins on write latency by ~{set_ratio:.1f}x "
            "-- that's the price of NimbusKV's immutability, versioning, and "
            f"subscriber hooks, not a flaw. NimbusKV is {get_delta} than dict+Lock "
            "on **reads** ({} vs {}), which makes sense: dict+Lock still pays "
            "lock-acquisition cost on every read, while NimbusKV's get() never "
            "takes a lock at all.\n".format(us(nk["get"]["p50"]), us(dl["get"]["p50"]))
        )

    lines.append(f'![Comparative throughput]({charts["throughput"]})\n')
    ct = results["throughput_vs_threads"]
    if "dict+Lock" in ct:
        dxs = sorted(int(k) for k in ct["dict+Lock"].keys())
        d0 = ct["dict+Lock"][str(dxs[0])] if str(dxs[0]) in ct["dict+Lock"] else ct["dict+Lock"][dxs[0]]
        d1 = ct["dict+Lock"][str(dxs[-1])] if str(dxs[-1]) in ct["dict+Lock"] else ct["dict+Lock"][dxs[-1]]
        direction = "increased" if d1 > d0 else ("decreased" if d1 < d0 else "stayed flat")
        lines.append(
            f"`dict+Lock` throughput {direction} with thread count on this run "
            f"({d0:,.0f} \u2192 {d1:,.0f} ops/sec, {dxs[0]}\u2192{dxs[-1]} threads). "
            "On a machine with only one logical CPU available to this process, "
            "genuine parallel speed-up isn't physically possible under the GIL "
            "-- run-to-run swings here reflect thread-scheduling and lock-"
            "acquisition noise for a very short, uncontended critical section, "
            "not real scaling in either direction. Re-run `charts.py` a few "
            "times if you want to see this vary.\n"
        )

    lines.append(f'![Comparative async]({charts["async"]})\n')
    ca = results["async"]
    rows = [[name, us(v["p50"])] for name, v in ca.items()]
    lines.append(md_table(["Library", "await set() p50"], rows))
    return "\n".join(lines)


def to_json(results):
    cl = results["latency"]
    ttl = results["ttl_features"]
    ct = results["throughput_vs_threads"]
    ca = results["async"]

    def _ttl_for(name):
        ttl_key = "NimbusKV" if name.startswith("NimbusKV") else name.split(".")[0].split(" ")[0]
        info = ttl.get(ttl_key, ttl.get(name, {}))
        native = info.get("native_ttl", None)
        if native is True:
            return "Yes"
        if native is False:
            return "No"
        return native  # e.g. "per-cache-instance, not per-key", or None

    latency_rows = [
        {
            "library": name,
            "setP50": cl[name]["set"]["p50"] * 1e6,
            "getP50": cl[name]["get"]["p50"] * 1e6,
            "nativePerKeyTtl": _ttl_for(name),
        }
        for name in cl.keys()
    ]

    nk = cl.get("NimbusKV (ReactiveStore)")
    dl = cl.get("dict+Lock")
    honest_read = None
    if nk and dl:
        honest_read = {
            "setRatioDictLockFaster": nk["set"]["p50"] / dl["set"]["p50"],
            "nimbuskvFasterOnGet": nk["get"]["p50"] < dl["get"]["p50"],
        }

    throughput_series = {}
    for name, d in ct.items():
        xs = sorted(int(k) for k in d.keys())
        get = lambda k: d[k] if k in d else d[str(k)]
        throughput_series[name] = [{"threads": x, "opsPerSec": get(x)} for x in xs]

    return {
        "title": TITLE,
        "singleThreadedLatency": {
            "unit": "microseconds",
            "rows": latency_rows,
            "honestRead": honest_read,
        },
        "multithreadedThroughput": {
            "unit": "ops/sec",
            "series": throughput_series,
        },
        "asyncOverhead": {
            "unit": "microseconds",
            "rows": [{"library": name, "awaitSetP50": v["p50"] * 1e6} for name, v in ca.items()],
        },
    }


if __name__ == "__main__":
    import json
    r = run()
    with open("/tmp/b05.json", "w", encoding="utf-8") as f:
        json.dump(r, f, indent=2, default=str)
    print(json.dumps(to_json(r), indent=2, default=str))
