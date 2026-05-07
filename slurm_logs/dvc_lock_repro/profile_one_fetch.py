#!/usr/bin/env python3
"""Profile a single cold ``_ensure_dvc_pulled()`` call with pyinstrument.

Goal: pinpoint where the ~93s wall-clock per cold fetch is being spent.
Hypothesis: DVC graph walk on Lustre dominates; S3 transfer is rounding
error.  Profile output should confirm or refute that.

Usage:
    python profile_one_fetch.py [--target <countries-relative path>]
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

DEFAULT_TARGET = "Uganda/2013-14/Data/gsec12.dta"


def _md5_for_target(repo_root: Path, target: str) -> str | None:
    import yaml
    sc = repo_root / "lsms_library" / "countries" / f"{target}.dvc"
    if not sc.exists():
        return None
    return yaml.safe_load(sc.read_text())["outs"][0]["md5"]


def main(argv):
    p = argparse.ArgumentParser()
    p.add_argument("--target", default=DEFAULT_TARGET)
    p.add_argument("--output", default=None,
                   help="Path for HTML profile output (default: ./profile_{target_md5}.html)")
    args = p.parse_args(argv)

    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent.parent.resolve()
    data_dir = (os.environ.get("LSMS_DATA_DIR")
                or str(Path.home() / ".local/share/lsms_library"))
    cache_dir = Path(data_dir).expanduser().resolve() / "dvc-cache"

    md5 = _md5_for_target(repo_root, args.target)
    if not md5:
        print(f"no .dvc sidecar for {args.target}", file=sys.stderr)
        sys.exit(2)

    # Force cold by clearing this target's cache slot
    for p in (cache_dir / md5[:2] / md5[2:],
              cache_dir / "files" / "md5" / md5[:2] / md5[2:]):
        if p.exists():
            p.unlink()
            print(f"cleared {p}")

    # GC any stale locks
    lock_dir = repo_root / "lsms_library" / "countries" / ".dvc" / "tmp"
    if lock_dir.exists():
        for p in lock_dir.glob("*lock*"):
            try:
                p.unlink()
                print(f"gc'd stale lock {p}")
            except OSError:
                pass

    # Now profile a single _ensure_dvc_pulled call.
    # Import lsms_library (cwd already inside the worktree, so the patched
    # version wins).
    sys.path.insert(0, str(repo_root))
    from lsms_library.local_tools import _ensure_dvc_pulled

    output = args.output or str(script_dir / f"profile_{md5[:8]}_{int(time.time())}.html")

    print(f"profiling _ensure_dvc_pulled({args.target!r})")
    print(f"  repo_root: {repo_root}")
    print(f"  md5: {md5}")
    print(f"  output: {output}")

    from pyinstrument import Profiler
    p = Profiler(interval=0.01, async_mode="disabled")
    t0 = time.perf_counter()
    p.start()
    try:
        _ensure_dvc_pulled(args.target)
    finally:
        p.stop()
    elapsed = time.perf_counter() - t0
    print(f"\n=== elapsed: {elapsed:.2f}s ===")
    print()
    print(p.output_text(unicode=True, color=False, show_all=True))
    Path(output).write_text(p.output_html())
    print(f"\nHTML profile written to: {output}")


if __name__ == "__main__":
    main(sys.argv[1:])
