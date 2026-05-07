#!/usr/bin/env python3
"""DVC lock-contention micro-reproducer.

Spawns N child processes that simultaneously call
``lsms_library.local_tools._ensure_dvc_pulled`` on a list of DVC-tracked
source files.  With a cleared cache (or per-blob cache wipe) every child
falls through to ``DVCFS.repo.fetch`` and therefore competes for the
single DVC repo lock at ``.dvc/tmp/lock``.

Usage:

    python repro_micro.py [--n 12] [--clear-cache] [--targets <file1> <file2> ...]

Reports:
- per-child elapsed time, exit status, exception class
- aggregate failure rate, success/fail counts
- machine-readable JSON to stdout (one final line) for downstream sweeps

The script does NOT change DVC config.  Wrap it from a shell driver
(``run_sweep.sh``) that toggles ``.dvc/config`` between runs to compare
configurations.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from multiprocessing import Pool
from pathlib import Path

# Default reproducer targets: 12 source files used by Uganda's earnings /
# enterprise_income chain, spread across waves so a single fetch can't
# satisfy all 12 from one cache hit.
DEFAULT_TARGETS = [
    "Uganda/2013-14/Data/gsec12.dta",
    "Uganda/2013-14/Data/GSEC8_1.dta",
    "Uganda/2013-14/Data/GSEC11A.dta",
    "Uganda/2013-14/Data/GSEC10_1.dta",
    "Uganda/2015-16/Data/gsec12_1.dta",
    "Uganda/2015-16/Data/gsec10_1.dta",
    "Uganda/2015-16/Data/gsec11_1.dta",
    "Uganda/2015-16/Data/AGSEC8A.dta",
    "Uganda/2019-20/Data/HH/gsec12_1.dta",
    "Uganda/2019-20/Data/HH/gsec8.dta",
    "Uganda/2019-20/Data/HH/gsec10_1.dta",
    "Uganda/2009-10/Data/GSEC12.dta",
]


def _md5_for_target(repo_root: Path, target: str) -> str | None:
    """Read the .dvc sidecar's md5 for a countries-relative target path."""
    import yaml

    sidecar = repo_root / "lsms_library" / "countries" / f"{target}.dvc"
    if not sidecar.exists():
        return None
    with sidecar.open() as fh:
        data = yaml.safe_load(fh)
    return data["outs"][0]["md5"]


def _cache_paths(cache_dir: Path, md5: str) -> list[Path]:
    return [
        cache_dir / md5[:2] / md5[2:],
        cache_dir / "files" / "md5" / md5[:2] / md5[2:],
    ]


def clear_cache_for_targets(repo_root: Path, cache_dir: Path, targets: list[str]) -> dict:
    """Remove any cache blobs corresponding to the given targets.

    Returns a dict ``{target: status}`` for the report.
    """
    out = {}
    for tgt in targets:
        md5 = _md5_for_target(repo_root, tgt)
        if md5 is None:
            out[tgt] = "no_sidecar"
            continue
        removed = []
        for p in _cache_paths(cache_dir, md5):
            if p.exists():
                try:
                    p.unlink()
                    removed.append(str(p))
                except OSError as e:
                    removed.append(f"!{p}:{e}")
        out[tgt] = ("cleared:" + ";".join(removed)) if removed else "miss"
    return out


def worker(args):
    """Child entrypoint: import library, fetch one target, return timing."""
    idx, target, repo_root_str = args
    sys.path.insert(0, repo_root_str)  # ensure correct lsms_library
    t0 = time.perf_counter()
    pid = os.getpid()
    try:
        from lsms_library.local_tools import _ensure_dvc_pulled

        _ensure_dvc_pulled(target)
        dt = time.perf_counter() - t0
        return {"idx": idx, "target": target, "pid": pid, "ok": True,
                "dt": dt, "exc": None, "msg": None}
    except BaseException as e:  # capture EVERYTHING including SystemExit
        dt = time.perf_counter() - t0
        return {"idx": idx, "target": target, "pid": pid, "ok": False,
                "dt": dt, "exc": type(e).__name__, "msg": str(e)[:300]}


def main(argv):
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, default=12,
                   help="Number of concurrent child processes (default 12)")
    p.add_argument("--clear-cache", action="store_true",
                   help="Remove cache blobs for the targets before running")
    p.add_argument("--targets", nargs="*", default=None,
                   help="Override default target list "
                        "(countries-relative paths)")
    p.add_argument("--cache-dir", default=None,
                   help="DVC cache dir (default: $LSMS_DATA_DIR/dvc-cache)")
    p.add_argument("--repo-root", default=None,
                   help="LSMS_Library repo root "
                        "(default: derived from this script's location)")
    p.add_argument("--label", default="run",
                   help="Label string echoed in the JSON output")
    args = p.parse_args(argv)

    script_dir = Path(__file__).resolve().parent
    repo_root = (Path(args.repo_root) if args.repo_root
                 else script_dir.parent.parent).resolve()

    if args.cache_dir:
        cache_dir = Path(args.cache_dir).expanduser().resolve()
    else:
        data_dir = (os.environ.get("LSMS_DATA_DIR")
                    or str(Path.home() / ".local/share/lsms_library"))
        cache_dir = Path(data_dir).expanduser().resolve() / "dvc-cache"

    targets = list(args.targets) if args.targets else list(DEFAULT_TARGETS)
    if args.n != len(targets):
        # Repeat / truncate targets to length n.  Distinct targets are
        # ideal, but if --n is larger, cycle through.
        if args.n > len(targets):
            targets = (targets * ((args.n // len(targets)) + 1))[:args.n]
        else:
            targets = targets[: args.n]

    cache_clear_report = None
    if args.clear_cache:
        cache_clear_report = clear_cache_for_targets(repo_root, cache_dir, targets)

    # Stale lock cleanup (in case a prior run left them).
    lock_dir = repo_root / "lsms_library" / "countries" / ".dvc" / "tmp"
    pre_locks = sorted(str(p) for p in lock_dir.glob("*lock*")) if lock_dir.exists() else []
    for lf in pre_locks:
        try:
            os.unlink(lf)
        except OSError:
            pass

    job_args = [(i, t, str(repo_root)) for i, t in enumerate(targets)]
    t0 = time.perf_counter()
    with Pool(args.n) as pool:
        # imap_unordered preserves first-error visibility but the worker
        # already swallows exceptions.  Use map for deterministic order.
        results = pool.map(worker, job_args)
    elapsed = time.perf_counter() - t0

    successes = [r for r in results if r["ok"]]
    failures = [r for r in results if not r["ok"]]

    # Human-readable summary
    print(f"=== {args.label} ===")
    print(f"n={args.n} elapsed={elapsed:.2f}s "
          f"successes={len(successes)} failures={len(failures)}")
    if successes:
        dts = sorted(r["dt"] for r in successes)
        print(f"  success_dt min={dts[0]:.2f} median={dts[len(dts)//2]:.2f} "
              f"max={dts[-1]:.2f}")
    if failures:
        from collections import Counter
        exc_counts = Counter(r["exc"] for r in failures)
        print(f"  failure_exc: {dict(exc_counts)}")
        # Show first failure detail for each exception class
        seen = set()
        for r in failures:
            if r["exc"] not in seen:
                print(f"    {r['exc']}: {r['msg']}")
                seen.add(r["exc"])

    # Machine-readable line
    summary = {
        "label": args.label,
        "n": args.n,
        "elapsed": elapsed,
        "successes": len(successes),
        "failures": len(failures),
        "fail_rate": (len(failures) / args.n) if args.n else 0.0,
        "exc_counts": dict(__import__("collections").Counter(
            r["exc"] for r in failures)),
        "results": results,
        "pre_locks_cleared": pre_locks,
        "cache_clear": cache_clear_report,
        "cache_dir": str(cache_dir),
    }
    print("RESULT_JSON " + json.dumps(summary))
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
