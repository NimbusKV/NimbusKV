# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""b06_write_path.py - write-path before/after benchmark (v3.0.1 vs 3.1.0).

Measures exactly what the 3.1.0 write-path changes target:
  * plain set() with no subscribers and no TTL, single thread
  * set() with one subscriber, single thread
  * hot-key atomic() increments across 1..32 threads

Run from the repo root:
    python bench/b06_write_path.py
    python -X gil=0 bench/b06_write_path.py     # free-threaded build

Point PYTHONPATH at another checkout to compare versions.
"""
import statistics
import sys
import threading
import time

import nimbuskv
from nimbuskv import NimbusKV

REPEATS = 5


def _median(fn):
    return statistics.median(fn() for _ in range(REPEATS))


def single_set(n=200_000, subscribed=False):
    ld = NimbusKV()
    if subscribed:
        ld.subscribe(lambda k, e, o, v: None)
    t0 = time.perf_counter()
    for i in range(n):
        ld.set("k%d" % (i & 1023), i)
    dt = time.perf_counter() - t0
    ld.stop()
    return n / dt


def hot_key_atomic(threads, total=200_000):
    ld = NimbusKV()
    ld.set("c", 0)
    per = total // threads
    barrier = threading.Barrier(threads + 1)

    def worker():
        barrier.wait()
        for _ in range(per):
            ld.atomic("c", lambda v: v + 1)

    ts = [threading.Thread(target=worker, daemon=True) for _ in range(threads)]
    for t in ts:
        t.start()
    barrier.wait()
    t0 = time.perf_counter()
    for t in ts:
        t.join()
    dt = time.perf_counter() - t0
    assert ld.get("c") == per * threads, "lost updates"
    ld.stop()
    return per * threads / dt


def main():
    gil = getattr(sys, "_is_gil_enabled", lambda: True)()
    print(f"nimbuskv {nimbuskv.__version__} from {nimbuskv.__file__}")
    print(f"Python {sys.version.split()[0]}  GIL enabled: {gil}")
    print(f"set(), no subs/TTL : {_median(single_set):>12,.0f} ops/sec")
    print(f"set(), 1 subscriber: {_median(lambda: single_set(subscribed=True)):>12,.0f} ops/sec")
    for n in (1, 2, 4, 8, 16, 32):
        print(f"atomic() hot key, {n:>2} threads: {_median(lambda: hot_key_atomic(n)):>12,.0f} ops/sec")


if __name__ == "__main__":
    main()
