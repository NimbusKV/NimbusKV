# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""b00_template_example.py

TEMPLATE for adding a new benchmark suite. Not registered in main.py by
default (it's the id "b00_template_example", not in the SUITES list) --
copy this file, rename it (e.g. bNN_your_test.py), fill it in, then add
one line to the SUITES list at the top of main.py:

    import bNN_your_test
    SUITES = [..., bNN_your_test]

That's the entire "registration" step. main.py will then, in order:
    1. call your_module.run()                      -> raw results
    2. save raw results to results/<SUITE_ID>.json
    3. call your_module.render(results, charts_dir) -> {key: chart_path}
    4. call your_module.to_markdown(results, charts)-> markdown string
    5. append that markdown as its own section of RESULTS.md
    6. call your_module.to_json(results)            -> JSON-serializable dict
    7. nest that dict under sections.<SUITE_ID> in report.json

Every suite file is completely independent -- it owns its own measuring,
its own charts, and its own markdown section. main.py never reaches
into your results dict; all the formatting logic belongs here.
"""
import time

from common import percentiles, log, save_chart, md_table, us

# ------------------------------------------------------------------ #
# 1. Required identity -- used for the results/*.json filename and the
#    section heading in RESULTS.md.
# ------------------------------------------------------------------ #
SUITE_ID = "b00_template_example"
TITLE = "0. Template Example (not registered -- copy me)"


# ------------------------------------------------------------------ #
# 2. Do your actual measuring here. Return anything JSON-serializable
#    (dict/list of numbers/strings). No file I/O, no matplotlib here.
# ------------------------------------------------------------------ #
def run():
    log("Running template example suite...")
    lat = []
    for _ in range(1000):
        t0 = time.perf_counter()
        time.sleep(0)  # replace with the real operation under test
        lat.append(time.perf_counter() - t0)
    return {"noop_latency": percentiles(lat)}


# ------------------------------------------------------------------ #
# 3. Generate this suite's chart(s). Use common.save_chart(fig, name)
#    -- it writes into the shared charts/ dir and returns the relative
#    path to embed in markdown. Return a dict of {logical_name: path}.
# ------------------------------------------------------------------ #
def render(results, charts_dir):
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(7, 4))
    d = results["noop_latency"]
    labels = ["p50", "p90", "p99"]
    vals = [d[l] * 1e6 for l in labels]
    ax.bar(labels, vals, color="#2563eb")
    ax.set_ylabel("latency (\u00b5s)")
    ax.set_title("Template example chart")
    path = save_chart(fig, "99_template_example.png")
    return {"example": path}


# ------------------------------------------------------------------ #
# 4. Build this suite's RESULTS.md section. Compute every number FROM
#    `results` -- never hardcode a figure, since results change every
#    run. Embed charts with markdown image syntax using the paths
#    returned by render().
# ------------------------------------------------------------------ #
def to_markdown(results, charts):
    lines = [f"## {TITLE}\n"]
    lines.append(f'![Template example]({charts["example"]})\n')
    d = results["noop_latency"]
    lines.append(md_table(["Percentile", "Latency"], [
        ["p50", us(d["p50"])],
        ["p90", us(d["p90"])],
        ["p99", us(d["p99"])],
    ]))
    return "\n".join(lines)


# ------------------------------------------------------------------ #
# 5. Same numbers as to_markdown(), shaped for report.json instead of
#    prose. Raw floats/ints, not pre-formatted strings -- a consumer
#    (e.g. a React chart) should be able to use these directly without
#    re-parsing "12.34µs" back into a number.
# ------------------------------------------------------------------ #
def to_json(results):
    d = results["noop_latency"]
    return {
        "title": TITLE,
        "noopLatencyUs": {
            "p50": d["p50"] * 1e6,
            "p90": d["p90"] * 1e6,
            "p99": d["p99"] * 1e6,
        },
    }


if __name__ == "__main__":
    r = run()
    charts = render(r, None)
    print(to_markdown(r, charts))
    print(to_json(r))
