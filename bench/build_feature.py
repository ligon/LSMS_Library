"""Time the cost of building a single (country, feature) parquet.

Usage:
    python bench/build_feature.py <Country> <feature> [--json out.jsonl]

Phases timed (perf_counter, seconds):
  1. import_lsms_library    -- one-time cost per process; on Lustre this
                                dominates a fresh ipython session.
  2. Country(name)          -- cheap reference; reads data_scheme.yml
  3. feature() #1           -- cold call.  For non-dvc.yaml countries this
                                rebuilds from source via load_from_waves.
                                For dvc.yaml countries this goes through
                                load_dataframe_with_dvc + stage layer.
  4. feature() #2           -- second call in the SAME process.  Measures
                                Country-instance ancillary caches
                                (_sample_v_cache, _updated_ids_cache,
                                _market_lookup_cache_*) plus OS page cache
                                benefit.  Should be much faster than #1.

Output:
  - Human-readable timing table to stdout
  - One-line JSON record (if --json is given) for aggregation across runs

To measure CROSS-process behavior, invoke this script twice in separate
subprocesses without clearing the data_root in between.  Per the v0.7.0
plan, the second-process first-call is the metric of interest: it should
be sub-second after the cache-read fix lands.

To measure COLD-rebuild behavior, clear ~/.local/share/lsms_library/<Country>/
before invoking.  See bench/run_bench.sh which automates the pattern.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
from datetime import datetime
from pathlib import Path


def _emit(label: str, seconds: float, extra: str = "") -> None:
    suffix = f"  ({extra})" if extra else ""
    print(f"  {label:42s} {seconds:8.3f}s{suffix}")


def time_step(label: str, fn):
    """Run fn(), record perf_counter delta, return (label, seconds, result)."""
    t0 = time.perf_counter()
    try:
        result = fn()
    except BaseException as exc:
        t1 = time.perf_counter()
        return label, t1 - t0, exc
    t1 = time.perf_counter()
    return label, t1 - t0, result


def df_summary(df) -> dict:
    """Compact summary of a DataFrame for the JSON record."""
    try:
        import pandas as pd  # local import; pd may not be loaded yet
    except ImportError:
        return {"error": "pandas not importable"}
    if not isinstance(df, pd.DataFrame):
        return {"type": type(df).__name__}
    summary = {
        "type": "DataFrame",
        "rows": int(len(df)),
        "ncols": int(df.shape[1]),
        "columns": [str(c) for c in df.columns][:8],
        "index_names": [str(n) for n in df.index.names],
    }
    if hasattr(df.index, "duplicated"):
        try:
            summary["dup_index_frac"] = float(df.index.duplicated().mean())
        except Exception:
            pass
    return summary


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="build_feature.py")
    parser.add_argument("country", help="Country name, e.g. Niger")
    parser.add_argument("feature", help="Feature method, e.g. household_roster")
    parser.add_argument(
        "--json",
        default=None,
        help="Append a one-line JSON record to this file",
    )
    parser.add_argument(
        "--label",
        default=None,
        help="Free-form label written into the JSON record (e.g. 'cold' or 'warm')",
    )
    args = parser.parse_args(argv)

    measurements: list[tuple[str, float]] = []

    # 1. import lsms_library
    label, elapsed, result = time_step(
        "import lsms_library",
        lambda: __import__("lsms_library"),
    )
    measurements.append((label, elapsed))
    _emit(label, elapsed)
    if isinstance(result, BaseException):
        print(f"FAILED at import: {result!r}", file=sys.stderr)
        return 1
    import lsms_library as ll  # noqa: E402

    # 2. Country construction
    label, elapsed, country = time_step(
        f"Country({args.country!r})",
        lambda: ll.Country(args.country),
    )
    measurements.append((label, elapsed))
    _emit(label, elapsed)
    if isinstance(country, BaseException):
        print(f"FAILED at Country construction: {country!r}", file=sys.stderr)
        return 1

    # 3. First feature call (cold)
    label, elapsed, df1 = time_step(
        f"{args.feature}() #1 (cold)",
        lambda: getattr(country, args.feature)(),
    )
    measurements.append((label, elapsed))
    _emit(label, elapsed)
    if isinstance(df1, BaseException):
        print(f"FAILED at {args.feature}() #1: {df1!r}", file=sys.stderr)
        return 1

    # 4. Second feature call (warm in-process)
    label, elapsed, df2 = time_step(
        f"{args.feature}() #2 (warm in-proc)",
        lambda: getattr(country, args.feature)(),
    )
    measurements.append((label, elapsed))
    _emit(label, elapsed)
    if isinstance(df2, BaseException):
        print(f"FAILED at {args.feature}() #2: {df2!r}", file=sys.stderr)
        return 1

    # Summary
    total = sum(secs for _, secs in measurements)
    print(f"  {'-' * 50}")
    _emit("TOTAL", total)
    print(f"  rows: {len(df1):,}  cols: {df1.shape[1]}")

    # JSON record (one line, append to file or print to stdout if no --json)
    record = {
        "country": args.country,
        "feature": args.feature,
        "label": args.label,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "host": socket.gethostname(),
        "python": sys.version.split()[0],
        "pid": os.getpid(),
        "lsms_library_path": str(Path(ll.__file__).resolve().parent),
        "data_dir_env": os.environ.get("LSMS_DATA_DIR", ""),
        "build_backend_env": os.environ.get("LSMS_BUILD_BACKEND", ""),
        "no_cache_env": os.environ.get("LSMS_NO_CACHE", ""),
        "trust_cache_env": os.environ.get("LSMS_TRUST_CACHE", ""),
        "steps": [
            {"label": label, "seconds": round(secs, 6)} for label, secs in measurements
        ],
        "total_seconds": round(total, 6),
        "result": df_summary(df1),
    }

    if args.json:
        Path(args.json).parent.mkdir(parents=True, exist_ok=True)
        with open(args.json, "a") as fh:
            fh.write(json.dumps(record) + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
