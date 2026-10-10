<!--
Thanks for the PR! A few things that make this much faster to review,
especially since a lot of this codebase is concurrency-sensitive.
-->

## What does this change?

<!-- One or two sentences. Link the issue it closes, if any. -->

## Why?

<!-- What problem does this solve, or what does it improve? -->

## Checklist

- [ ] `python3 -m pytest tests/` passes locally
- [ ] If this touches `reactive_core.py`, `ttl.py`, or anything on
      the write/CAS path: I added or updated a test that would catch
      a regression here specifically (a plain "it works on my
      machine" isn't enough for anything concurrency-related — see
      `tests/` for examples using `threading.Barrier` +
      `sys.setswitchinterval` to force interleavings deterministically)
- [ ] If this changes performance-relevant code: I ran
      `bench/b06_write_path.py` before/after and
      can share the numbers below
- [ ] `CHANGELOG.md` updated, if this is user-facing
- [ ] No new dependency added without discussion (or: added and
      justified below). Any new dependency has an Apache-2.0-compatible
      license (MIT, BSD, Apache-2.0, etc.; not GPL)
- [ ] I have signed the [CLA](../CLA.md) (first-time contributors:
      comment "I have read the NimbusKV CLA (version 1.0) and I agree
      to its terms.")

## Benchmark results (if applicable)

<!-- Paste before/after numbers here. "No perf-sensitive code touched" is a fine answer too. -->

## Anything reviewers should look at closely?

<!-- E.g. "not 100% sure this backoff tuning is right" — flag your own uncertainty, it helps. -->
