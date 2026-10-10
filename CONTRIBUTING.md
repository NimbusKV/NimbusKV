# Contributing to NimbusKV

Thanks for considering it. NimbusKV is a solo-maintained project, so contributions - bug reports, PRs, and design feedback - genuinely help.

## Before you start

For anything beyond a small fix (typo, docstring, a single bug), open an issue first describing what you want to change and why. NimbusKV's core is a concurrency engine - changes there need to be reasoned about carefully, so it's worth agreeing on the approach before writing code.

## Setup

```bash
git clone https://github.com/hemanshu03/NimbusKV.git
cd NimbusKV
pip install -e ".[dev]"
```

This installs NimbusKV in editable mode plus `pytest`, `pytest-asyncio`, and `fakeredis` for the test suite.

## Running tests

```bash
pytest tests/ -v
```

If you have a free-threaded Python build (3.13t/3.14t) available, please also run the suite there - that's the runtime this library's core promise is actually about, and standard-Python-only testing can't catch every concurrency issue.

```bash
python bench/b06_write_path.py
```

benchmarks the write path (plain `set()`, `set()` with a subscriber, hot-key `atomic()` across 1-32 threads) - run it before and after any change that could affect performance.

## Code style

- Every public class/method needs a docstring: what it does, `Args`/`Returns`/`Raises` where relevant, and a runnable `Example`. Look at any existing method in `nimbuskv/modules/nimbuskv_v3.py` for the expected format.
- Functions passed to `ReactiveStore.mutate()` / `atomic()` run exactly once, under the store's write lock. Keep them short, and never write to the same store from inside one (that raises `ReentrantWriteError`).
- Keep the persistence backend contract intact: `PersistenceBackend` implementations are write-through mirrors only, never read from directly by the hot path. Don't add a fast-path read from a backend, even as an optimization - it breaks the lock-free read guarantee.

## What's especially useful right now

- **Free-threaded (3.13t/3.14t) testing and bug reports.** This is the newest, least-battle-tested part of the runtime story, and the area most likely to have real bugs no one's found yet.
- **Real sandboxing design.** A previous "sandbox" feature was removed because it wasn't actually isolating anything (just a timeout wrapper). If you have experience with subprocess isolation or WASM-based sandboxing in Python, this would be a genuinely valuable contribution.
- **Additional persistence backends** beyond Memory/SQLite/Redis, following the `PersistenceBackend` interface.

## Reporting bugs

Open a [GitHub issue](https://github.com/hemanshu03/NimbusKV/issues) with:
- What you expected vs. what happened
- A minimal reproduction if possible
- Your Python version, and whether it's a free-threaded build (`python -c "import sys; print(sys._is_gil_enabled())"` if on 3.13+)

## License

NimbusKV is licensed under the [Apache License 2.0](LICENSE).

Before your first pull request can be merged, you need to agree to the [Contributor License Agreement](CLA.md) by commenting on your pull request:

> I have read the NimbusKV CLA (version 1.0) and I agree to its terms.

You keep the copyright in your work. The CLA gives the project the rights it needs to keep NimbusKV available under Apache-2.0 and, if needed, under other terms in the future. Contributions made before 3.1.0 were accepted under GPL-3.0.
