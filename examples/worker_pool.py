# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""worker_pool.py - producer/consumer coordination without polling.

This is the example that actually demonstrates why NimbusKV's reactivity
matters, not just its caching: multiple worker threads pick up jobs and
report results, a dispatcher waits on completion with wait_for() instead
of a sleep-and-poll loop, and atomic() coordinates a shared "jobs done"
counter safely across threads.

Run: python examples/worker_pool.py
"""
import threading
import time

from nimbuskv import NimbusKV


def worker(store: NimbusKV, worker_id: int, job_queue: list, lock: threading.Lock):
    """A worker thread that pulls jobs off a shared list and reports
    results into the store. (The job queue itself uses a plain lock
    here for simplicity -- NimbusKV isn't a queue, it's the shared
    state / coordination layer around one.)
    """
    while True:
        with lock:
            if not job_queue:
                return
            job_id = job_queue.pop()

        time.sleep(0.1)  # simulate work
        result = job_id * job_id

        store.set(f"job:{job_id}:result", result)
        store.atomic("jobs_completed", lambda v: (v or 0) + 1)
        print(f"[worker {worker_id}] finished job {job_id} -> {result}")


def main():
    with NimbusKV() as store:
        store.set("jobs_completed", 0)

        n_jobs = 8
        job_queue = list(range(1, n_jobs + 1))
        queue_lock = threading.Lock()

        workers = [
            threading.Thread(target=worker, args=(store, i, job_queue, queue_lock))
            for i in range(3)
        ]
        for w in workers:
            w.start()

        # The dispatcher doesn't poll or sleep-loop to know when all jobs
        # are done -- it blocks on wait_for(), which is driven by the
        # subscription system and unblocks the instant the condition is
        # actually met.
        print("Dispatcher waiting for all jobs to complete...")
        store.wait_for("jobs_completed", lambda v: v == n_jobs, timeout=30)
        print("All jobs completed.")

        for w in workers:
            w.join()

        results = {job_id: store.get(f"job:{job_id}:result") for job_id in range(1, n_jobs + 1)}
        print("Results:", results)


if __name__ == "__main__":
    main()
