#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""main.py - the ONE script you run.

    python3 main.py

Runs every registered benchmark suite against the installed NimbusKV,
generates every chart, and writes a single RESULTS.md (with charts
embedded) summarizing everything -- properly formatted, every number
computed live from what actually ran, nothing hardcoded.

============================== ON EVERY RELEASE ==============================
Bump VERSION below before you run this. That's it -- it's stamped into
the RESULTS.md header and into results/meta.json so old reports don't
get confused for new ones.

============================== ADDING A NEW TEST ==============================
1. Copy b00_template_example.py -> bNN_your_test.py (see that file for
   the full contract: SUITE_ID, TITLE, run(), render(), to_markdown()).
2. Import it below and add it to the SUITES list. That's the whole
   "registration" step -- main.py does not need any other changes.
================================================================================
"""

import importlib
import json
import os
import sys
import time
import traceback
import matplotlib

matplotlib.use("Agg")  # no X server needed for chart rendering

# --------------------------------------------------------------------------
VERSION = "1.0.0"
# --------------------------------------------------------------------------

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import common  # noqa: E402

# ---- registered suites, run in this order -------------------------------
import b01_correctness
import b02_throughput_latency
import b03_scalability_memory_ttl_subscribe
import b04_async_persistence_breaking
import b05_comparative

SUITES = [
    b01_correctness,
    b02_throughput_latency,
    b03_scalability_memory_ttl_subscribe,
    b04_async_persistence_breaking,
    b05_comparative,
]
# To add a new suite: write bNN_your_test.py (copy b00_template_example.py),
# `import bNN_your_test` above, and add it to this list. Nothing else
# in this file needs to change.
# ---------------------------------------------------------------------------


REPORT_PATH = os.path.join(common.BENCH_DIR, "RESULTS.md")


def _validate_suite(mod):
    required = ["SUITE_ID", "TITLE", "run", "render", "to_markdown", "to_json"]
    missing = [r for r in required if not hasattr(mod, r)]
    if missing:
        raise RuntimeError(
            f"{mod.__name__} is missing required suite attributes: {missing}. "
            f"See b00_template_example.py for the contract."
        )


def _make_summary_dashboard(all_results, sysinfo):
    """A single at-a-glance chart built from whatever suites actually
    ran, using only fields that are reliably present (correctness
    pass/fail counts, scalability curve if available)."""
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))

    ax = axes[0]
    ax.axis("off")
    correctness = all_results.get("b01_correctness")
    if correctness:
        n_pass = sum(1 for r in correctness if r.get("passed"))
        n_total = len(correctness)
        color = "#10b981" if n_pass == n_total else "#ef4444"
        ax.text(
            0.5,
            0.6,
            f"{n_pass}/{n_total}",
            ha="center",
            fontsize=44,
            fontweight="bold",
            color=color,
        )
        ax.text(0.5, 0.32, "correctness checks passed", ha="center", fontsize=12)
    else:
        ax.text(0.5, 0.5, "(no correctness suite registered)", ha="center", fontsize=10)

    ax = axes[1]
    scal = all_results.get("b03_scalability_memory_ttl_subscribe", {}).get(
        "scalability_vs_keycount"
    )
    if scal:
        xs = sorted(int(k) for k in scal.keys())

        def _v(x):
            return scal[x] if x in scal else scal[str(x)]

        ys = [_v(x)["get"]["p50"] * 1e6 for x in xs]
        ax.plot(xs, ys, marker="o", color="#2563eb")
        ax.set_xscale("log")
        ax.set_xlabel("keys in store")
        ax.set_ylabel("get() p50 (\u00b5s)")
        ax.set_title("Scalability")
    else:
        ax.axis("off")
        ax.text(0.5, 0.5, "(no scalability data)", ha="center", fontsize=10)

    footer = (
        f"NimbusKV benchmark suite v{VERSION} | {sysinfo['logical_cpus']} logical CPU(s), "
        f"{sysinfo['platform']} | {sysinfo['python_implementation']} "
        f"{sysinfo['python_version']}, "
        f"{'GIL disabled' if sysinfo['gil_disabled_build'] else 'GIL enabled'}"
    )
    return common.save_chart(fig, "00_summary_dashboard.png", footer_text=footer)


def main():
    t_start = time.time()
    sysinfo = common.system_info()
    common.log(f"NimbusKV benchmark suite v{VERSION}")
    common.log(
        f"System: {sysinfo['logical_cpus']} logical CPU(s), "
        f"{sysinfo['python_implementation']} {sysinfo['python_version']}, "
        f"GIL {'disabled' if sysinfo['gil_disabled_build'] else 'enabled'}"
    )

    try:
        import nimbuskv

        nimbuskv_version = getattr(nimbuskv, "__version__", "unknown")
    except ImportError:
        nimbuskv_version = "NOT INSTALLED"

    all_results = {}
    all_charts = {}
    all_markdown = {}
    all_json = {}
    failures = []

    for mod in SUITES:
        _validate_suite(mod)
        common.log(f"=== {mod.SUITE_ID}: {mod.TITLE} ===")
        try:
            results = mod.run()
            common.save_json(mod.SUITE_ID, results)
            charts = mod.render(results, common.CHARTS_DIR)
            md = mod.to_markdown(results, charts)
            js = mod.to_json(results)
            all_results[mod.SUITE_ID] = results
            all_charts[mod.SUITE_ID] = charts
            all_markdown[mod.SUITE_ID] = md
            all_json[mod.SUITE_ID] = js
        except Exception as e:
            common.log(f"  !! {mod.SUITE_ID} FAILED: {e}")
            traceback.print_exc()
            failures.append((mod.SUITE_ID, str(e)))
            all_markdown[mod.SUITE_ID] = (
                f"## {getattr(mod, 'TITLE', mod.SUITE_ID)}\n\n"
                f"**This suite failed to complete:** `{e}`\n\n"
                f"See console output for the full traceback. Results below "
                f"reflect every other suite that did complete.\n"
            )
            all_json[mod.SUITE_ID] = {
                "title": getattr(mod, "TITLE", mod.SUITE_ID),
                "failed": True,
                "error": str(e),
            }

    dashboard_path = _make_summary_dashboard(all_results, sysinfo)

    common.save_json(
        "meta",
        {
            "version": VERSION,
            "nimbuskv_version": nimbuskv_version,
            "system": sysinfo,
            "suites_run": [m.SUITE_ID for m in SUITES],
            "failures": failures,
            "elapsed_s": round(time.time() - t_start, 1),
        },
    )

    _write_results_md(sysinfo, nimbuskv_version, all_markdown, dashboard_path, failures)
    report_json_path = _write_report_json(sysinfo, nimbuskv_version, all_json, failures)

    elapsed = time.time() - t_start
    common.log(f"Done in {elapsed:.1f}s. Wrote {REPORT_PATH} and {report_json_path}")
    if failures:
        common.log(f"NOTE: {len(failures)} suite(s) failed: {[f[0] for f in failures]}")


def _write_report_json(sysinfo, nimbuskv_version, all_json, failures):
    """Consolidated, machine-readable twin of RESULTS.md. Every suite's
    to_json(results) output is nested under sections.<SUITE_ID> exactly
    as that suite produced it -- main.py doesn't reshape or recompute
    anything here, same rule as _write_results_md. sysinfo is redacted
    (see common.redact_sysinfo) since this file is meant to be shared/
    rendered outside this machine; results/meta.json still has the full,
    unredacted sysinfo for local use."""
    payload = {
        "meta": {
            "reportTitle": "NimbusKV Benchmark Results",
            "version": VERSION,
            "nimbuskvVersion": nimbuskv_version,
            "generatedAt": sysinfo["timestamp"],
            "note": (
                "Every number in this report came from actually running "
                "NimbusKV's real code on this machine."
            ),
            "system": common.redact_sysinfo(sysinfo),
            "singleCoreCaveat": bool(
                sysinfo["logical_cpus"] and sysinfo["logical_cpus"] <= 1
            ),
            "failures": [{"suiteId": sid, "error": err} for sid, err in failures],
        },
        "sections": all_json,
        "runningThisYourself": {
            "install": "clone this repo, and install all required dependencies.",
            "run": "cd bench -> python3 main.py / python -X gil=0 main.py",
            "note": "While contributing, bump VERSION at the top of main.py before each run you want to keep distinct. Raw numbers behind every chart are in results/*.json. To add a new test, see b00_template_example.py.",
        },
    }
    return common.write_report_json(payload)


def _write_results_md(
    sysinfo, nimbuskv_version, all_markdown, dashboard_path, failures
):
    lines = []
    lines.append(f"# NimbusKV Benchmark Results \u2014 v{VERSION}\n")
    lines.append(
        f"*Generated {sysinfo['timestamp']} against nimbuskv=={nimbuskv_version}. "
        "Every number in this report came from actually running NimbusKV's "
        "real code on this machine -- nothing here is estimated.*\n"
    )

    lines.append("## System under test\n")
    lines.append(
        common.md_table(
            ["Field", "Value"],
            [
                ["Logical CPUs", sysinfo["logical_cpus"]],
                ["Physical CPUs", sysinfo["physical_cpus"]],
                ["Platform", sysinfo["platform"]],
                ["Processor", sysinfo["processor"]],
                ["Total RAM", f"{sysinfo['total_mem_gb']} GB"],
                [
                    "Python",
                    f"{sysinfo['python_implementation']} {sysinfo['python_version']}",
                ],
                [
                    "GIL",
                    (
                        "disabled (free-threaded build)"
                        if sysinfo["gil_disabled_build"]
                        else "enabled (standard build)"
                    ),
                ],
                ["nimbuskv version", nimbuskv_version],
            ],
        )
    )
    lines.append("")

    if sysinfo["logical_cpus"] and sysinfo["logical_cpus"] <= 1:
        lines.append(
            "> **Single-core caveat:** this machine has only 1 logical CPU. "
            "Multithreaded results in this report show contention/overhead "
            "effects (locking, retries, scheduling) but cannot show real "
            "parallel speed-up -- there's no second core for a second thread "
            "to run on. Run this suite on multi-core hardware (and, ideally, "
            "a free-threaded Python 3.13t/3.14t build) to see the other half "
            "of the picture.\n"
        )

    if failures:
        lines.append(
            f"> **{len(failures)} suite(s) failed this run:** "
            f"{', '.join(f[0] for f in failures)}. See their sections below for details.\n"
        )

    lines.append("## Summary\n")
    lines.append(f"![Summary dashboard]({dashboard_path})\n")

    lines.append("## Contents\n")
    for mod in SUITES:
        anchor = (
            mod.TITLE.lower()
            .replace(" ", "-")
            .replace(",", "")
            .replace("&", "")
            .replace(":", "")
        )
        anchor = "".join(c for c in anchor if c.isalnum() or c == "-")
        lines.append(f"- [{mod.TITLE}](#{anchor})")
    lines.append("")

    for mod in SUITES:
        lines.append(all_markdown.get(mod.SUITE_ID, f"## {mod.TITLE}\n\n(no output)\n"))
        lines.append("\n---\n")

    lines.append(
        "## Running this yourself\n\n"
        "```bash\n"
        "pip install -e . matplotlib cachetools aiocache fakeredis psutil numpy --break-system-packages\n"
        "cd bench\n"
        "python3 main.py\n"
        "```\n\n"
        f"Bump `VERSION` at the top of `main.py` before each run you want "
        "to keep distinct. Raw numbers behind every chart are in "
        "`results/*.json`. To add a new test, see `b00_template_example.py`.\n"
    )

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    main()
