# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""common.py - shared helpers for the NimbusKV benchmark suite.

THE SUITE CONTRACT
===================
Every benchmark file (b01_correctness.py, b02_..., your new bNN_....py)
is a "suite" and must expose exactly these four names at module level:

    SUITE_ID    : str   short slug, used for the results/<SUITE_ID>.json
                  filename and as an anchor in RESULTS.md. Keep it
                  filesystem-safe (letters, digits, underscore).
    TITLE       : str   human-readable section heading for RESULTS.md.
    run()       -> dict/list, JSON-serializable. Do the actual
                  measuring here. No file I/O, no chart generation.
    render(results, charts_dir) -> dict[str, str]
                  Generate this suite's PNG chart(s) into charts_dir
                  (use common.save_chart) and return a dict mapping a
                  short chart key -> the RELATIVE path to embed in
                  markdown (e.g. "charts/01_correctness_results.png").
    to_markdown(results, charts) -> str
                  Build this suite's full RESULTS.md section: heading,
                  narrative, tables computed FROM `results` (never
                  hardcoded numbers), and `![...](...)` embeds using
                  the paths returned by render().
    to_json(results) -> dict, JSON-serializable.
                  Build this suite's section of report.json: the same
                  numbers as to_markdown(), structured for a machine
                  reader (e.g. a React page) instead of prose. Compute
                  everything FROM `results`, same rule as to_markdown --
                  never hardcode a figure. This is the ONLY function
                  that should shape data for report.json; main.py just
                  collects each suite's dict under its SUITE_ID and
                  writes the file, it never reaches into `results`
                  itself. Keep field names stable across runs (don't
                  rename keys based on what happened to be measured)
                  so a consumer can rely on the shape.

main.py imports each suite module, calls run() -> render() ->
to_markdown() -> to_json() in order, and stitches the markdown into
RESULTS.md and the json into report.json. To add a new suite: write
bNN_yourtest.py following this contract, then add one line to the
SUITES list at the top of main.py. See b00_template_example.py for a
minimal skeleton.
"""
import gc
import json
import logging
import os
import platform
import statistics
import sys
import sysconfig
import time

import psutil

# NimbusKV logs every subscriber exception at ERROR level with a full
# traceback. Several correctness/robustness tests deliberately throw
# from subscribers thousands of times -- silence the library logger;
# we assert on return values, not log lines.
logging.getLogger("nimbuskv").setLevel(logging.CRITICAL)

BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BENCH_DIR, "results")
CHARTS_DIR = os.path.join(BENCH_DIR, "charts")
REPORT_JSON_PATH = os.path.join(BENCH_DIR, "report.json")
os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(CHARTS_DIR, exist_ok=True)

_proc = psutil.Process(os.getpid())


# ------------------------------------------------------------------ #
# system / process introspection
# ------------------------------------------------------------------ #

def system_info() -> dict:
    return {
        "python_version": sys.version.split()[0],
        "python_implementation": platform.python_implementation(),
        "gil_disabled_build": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
        "gil_enabled_runtime": bool(getattr(sys, "_is_gil_enabled", lambda: True)()),
        "platform": platform.platform(),
        "processor": platform.processor() or platform.machine(),
        "logical_cpus": os.cpu_count(),
        "physical_cpus": psutil.cpu_count(logical=False),
        "total_mem_gb": round(psutil.virtual_memory().total / 1e9, 2),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S %Z"),
    }


def redact_sysinfo(sysinfo: dict) -> dict:
    """Generalize the fields of system_info() that identify the specific
    physical machine, for any output meant to leave it (e.g. report.json,
    which is designed to be committed/shared/rendered elsewhere). The full
    sysinfo dict is still kept as-is in results/meta.json for local use.

    - `processor` (exact CPU model/stepping string) is dropped to just the
      machine architecture.
    - `platform` (which embeds the exact OS build/kernel number) is
      collapsed to the OS family/major version.
    Everything else (core counts, RAM, Python/GIL info, timestamp) isn't
    considered device-identifying and is passed through unchanged.
    """
    out = dict(sysinfo)
    out["processor"] = platform.machine() or "redacted"
    out["platform"] = f"{platform.system()} {platform.release()}".strip() or "redacted"
    return out


def rss_mb() -> float:
    return _proc.memory_info().rss / 1e6


def gc_pause():
    gc.collect()
    gc.collect()


# ------------------------------------------------------------------ #
# measurement helpers
# ------------------------------------------------------------------ #

def percentiles(samples, ps=(50, 90, 99, 99.9)):
    if not samples:
        return {f"p{p}": None for p in ps}
    s = sorted(samples)
    out = {}
    n = len(s)
    for p in ps:
        idx = min(n - 1, int(round((p / 100.0) * (n - 1))))
        out[f"p{p}"] = s[idx]
    out["min"] = s[0]
    out["max"] = s[-1]
    out["mean"] = statistics.fmean(s)
    return out


def save_json(name, payload):
    path = os.path.join(RESULTS_DIR, f"{name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
    return path


def write_report_json(payload):
    """Write the consolidated, JSON-Schema'd report.json (see
    REPORT_JSON_PATH). Unlike save_json(), this uses ensure_ascii=False
    since this file is meant to be read by other tools/UIs (e.g. a React
    page) that render UTF-8 fine and shouldn't see \\u2192-style escapes."""
    with open(REPORT_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str, ensure_ascii=False)
    return REPORT_JSON_PATH


def log(msg):
    print(f"[bench] {msg}", flush=True)


# ------------------------------------------------------------------ #
# chart + markdown helpers (used by every suite's render()/to_markdown())
# ------------------------------------------------------------------ #

def save_chart(fig, filename, footer_text=None):
    """Save a matplotlib figure into CHARTS_DIR and return the path to
    embed in RESULTS.md, relative to the report (i.e. 'charts/<file>')."""
    import matplotlib.pyplot as plt
    if footer_text:
        fig.text(0.5, 0.005, footer_text, ha="center", fontsize=7, color="#888")
    fig.tight_layout(rect=[0, 0.02, 1, 1])
    path = os.path.join(CHARTS_DIR, filename)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return f"charts/{filename}"


def us(seconds):
    """Format a seconds value as a microsecond string for tables."""
    if seconds is None:
        return "\u2014"
    return f"{seconds * 1e6:,.2f}\u00b5s"


def ms(seconds):
    if seconds is None:
        return "\u2014"
    return f"{seconds * 1e3:,.2f}ms"


def md_table(headers, rows):
    """rows: list of lists of already-stringified cells."""
    out = ["| " + " | ".join(headers) + " |",
           "|" + "|".join(["---"] * len(headers)) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(c) for c in r) + " |")
    return "\n".join(out)


def img(path, alt=""):
    return f"![{alt}]({path})"
