#!/usr/bin/env python3
"""Benchmark DVC fetch timings for tuning the retry/backoff fix.

Captures per-file wall-clock for ``_ensure_dvc_pulled()`` against a set
of DVC-tracked Uganda source files of varying sizes, in three modes:

1. ``cold-cold``: clear each blob, run fetch in a fresh Python process
   per target.  Reflects the worst case — every wave-script's first
   call after a fresh cache.
2. ``cold-warm-process``: clear all blobs, run all fetches in a single
   Python process (each fetch is cold-blob but DVCFS / Repo is reused).
   Measures whether the index work amortizes in-process.
3. ``lock-only``: warm cache, just acquire+release ``repo.lock`` N times
   in a single process.  Floor on fetch overhead independent of S3.

Output: TSV to stdout, one row per fetch, columns:
    mode  trial  target  size_bytes  elapsed_s  ok  exc

Usage:
    python bench_fetch.py [--targets <file>...] [--modes cold-cold,...]
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

# Reuse the same default targets as the contention reproducer
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
    import yaml
    sidecar = repo_root / "lsms_library" / "countries" / f"{target}.dvc"
    if not sidecar.exists():
        return None
    with sidecar.open() as fh:
        return yaml.safe_load(fh)["outs"][0]["md5"]


def _file_size_bytes(repo_root: Path, target: str) -> int:
    """Read size from the .dvc sidecar if present, else the workspace file."""
    import yaml
    sidecar = repo_root / "lsms_library" / "countries" / f"{target}.dvc"
    if sidecar.exists():
        try:
            data = yaml.safe_load(sidecar.read_text())
            sz = data["outs"][0].get("size")
            if isinstance(sz, int):
                return sz
        except (yaml.YAMLError, KeyError, OSError):
            pass
    return -1


def _cache_paths(cache_dir: Path, md5: str) -> list[Path]:
    return [
        cache_dir / md5[:2] / md5[2:],
        cache_dir / "files" / "md5" / md5[:2] / md5[2:],
    ]


def _clear_blob(cache_dir: Path, md5: str) -> None:
    for p in _cache_paths(cache_dir, md5):
        if p.exists():
            try:
                p.unlink()
            except OSError:
                pass


def _emit(mode, trial, target, size, elapsed, ok, exc):
    print(f"{mode}\t{trial}\t{target}\t{size}\t{elapsed:.3f}\t"
          f"{'1' if ok else '0'}\t{exc or ''}",
          flush=True)


def run_cold_cold(repo_root, cache_dir, targets, trials):
    """Each fetch in its own process; cache cleared between fetches."""
    for trial in range(trials):
        for tgt in targets:
            md5 = _md5_for_target(repo_root, tgt)
            if md5:
                _clear_blob(cache_dir, md5)
            size = _file_size_bytes(repo_root, tgt)
            # Subprocess so we get a fresh Python process / fresh DVCFS each time.
            t0 = time.perf_counter()
            proc = subprocess.run(
                [sys.executable, "-c",
                 "import sys, time\n"
                 "from lsms_library.local_tools import _ensure_dvc_pulled\n"
                 "t = time.perf_counter()\n"
                 "_ensure_dvc_pulled(sys.argv[1])\n"
                 "print(time.perf_counter()-t, flush=True)\n",
                 tgt],
                capture_output=True, text=True,
                cwd=str(repo_root),
            )
            elapsed = time.perf_counter() - t0  # wall-clock includes process startup
            ok = proc.returncode == 0
            exc = (proc.stderr.splitlines()[-1] if proc.stderr.strip() else "") if not ok else ""
            _emit("cold-cold", trial, tgt, size, elapsed, ok, exc)


def run_cold_warm_process(repo_root, cache_dir, targets, trials):
    """All fetches in a single subprocess; cache cleared up front per trial."""
    for trial in range(trials):
        # Clear all blobs before this trial
        for tgt in targets:
            md5 = _md5_for_target(repo_root, tgt)
            if md5:
                _clear_blob(cache_dir, md5)
        # One subprocess for the whole trial — DVCFS is module-level, so
        # subsequent fetches benefit from any in-process caching.
        script = (
            "import sys, time, json\n"
            "from lsms_library.local_tools import _ensure_dvc_pulled\n"
            "for tgt in sys.argv[1:]:\n"
            "    t = time.perf_counter()\n"
            "    ok = True; exc = ''\n"
            "    try:\n"
            "        _ensure_dvc_pulled(tgt)\n"
            "    except BaseException as e:\n"
            "        ok = False; exc = type(e).__name__\n"
            "    dt = time.perf_counter() - t\n"
            "    print(json.dumps([tgt, dt, ok, exc]), flush=True)\n"
        )
        proc = subprocess.Popen(
            [sys.executable, "-c", script, *targets],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, cwd=str(repo_root),
        )
        # Stream and emit as each line arrives
        import json as _json
        try:
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                tgt, dt, ok, exc = _json.loads(line)
                size = _file_size_bytes(repo_root, tgt)
                _emit("cold-warm-process", trial, tgt, size, dt, ok, exc)
            proc.wait(timeout=1800)
        except subprocess.TimeoutExpired:
            proc.kill()
            print(f"# TIMEOUT in cold-warm-process trial {trial}", file=sys.stderr)


def run_lock_only(repo_root, cache_dir, targets, trials):
    """Warm cache; just acquire+release repo.lock N times in one process."""
    # Targets must already be cached for this mode to be meaningful
    script = (
        "import sys, time, json\n"
        "from lsms_library.local_tools import DVCFS\n"
        "n = int(sys.argv[1])\n"
        "repo = DVCFS.repo  # forces lazy build of the repo handle\n"
        "for i in range(n):\n"
        "    t = time.perf_counter()\n"
        "    with repo.lock:\n"
        "        pass\n"
        "    dt = time.perf_counter() - t\n"
        "    print(json.dumps([i, dt]), flush=True)\n"
    )
    n = len(targets) * trials
    proc = subprocess.Popen(
        [sys.executable, "-c", script, str(n)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, cwd=str(repo_root),
    )
    import json as _json
    try:
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            i, dt = _json.loads(line)
            _emit("lock-only", 0, f"acquire#{i}", 0, dt, True, "")
        proc.wait(timeout=300)
    except subprocess.TimeoutExpired:
        proc.kill()


def main(argv):
    p = argparse.ArgumentParser()
    p.add_argument("--targets", nargs="*", default=None)
    p.add_argument("--trials", type=int, default=1)
    p.add_argument("--modes", default="lock-only,cold-warm-process,cold-cold",
                   help="Comma-separated subset of: lock-only, cold-warm-process, cold-cold")
    args = p.parse_args(argv)

    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent.parent.resolve()
    data_dir = (os.environ.get("LSMS_DATA_DIR")
                or str(Path.home() / ".local/share/lsms_library"))
    cache_dir = Path(data_dir).expanduser().resolve() / "dvc-cache"

    targets = list(args.targets) if args.targets else list(DEFAULT_TARGETS)

    print("mode\ttrial\ttarget\tsize_bytes\telapsed_s\tok\texc", flush=True)
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    for mode in modes:
        if mode == "cold-cold":
            run_cold_cold(repo_root, cache_dir, targets, args.trials)
        elif mode == "cold-warm-process":
            run_cold_warm_process(repo_root, cache_dir, targets, args.trials)
        elif mode == "lock-only":
            run_lock_only(repo_root, cache_dir, targets, args.trials)
        else:
            print(f"# unknown mode: {mode}", file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv[1:])
