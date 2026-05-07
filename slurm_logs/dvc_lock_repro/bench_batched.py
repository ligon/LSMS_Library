#!/usr/bin/env python3
"""Bench the batched-fetch shape that the primary fix uses.

Compares:
  per-target:  N separate ``DVCFS.repo.fetch(targets=[t], jobs=1)`` calls
  batched-1:   one ``DVCFS.repo.fetch(targets=[all], jobs=1)`` call
  batched-4:   one ``DVCFS.repo.fetch(targets=[all], jobs=4)`` call
  batched-8:   one ``DVCFS.repo.fetch(targets=[all], jobs=8)`` call

All in a single Python process.  Cache is cleared for the whole target
list before each mode.  Output: TSV row per mode with total wall-clock.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

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
    sc = repo_root / "lsms_library" / "countries" / f"{target}.dvc"
    if not sc.exists():
        return None
    return yaml.safe_load(sc.read_text())["outs"][0]["md5"]


def _clear_blob(cache_dir: Path, md5: str) -> None:
    for p in (cache_dir / md5[:2] / md5[2:],
              cache_dir / "files/md5" / md5[:2] / md5[2:]):
        if p.exists():
            try:
                p.unlink()
            except OSError:
                pass


def _clear_all(repo_root: Path, cache_dir: Path, targets) -> None:
    for t in targets:
        md5 = _md5_for_target(repo_root, t)
        if md5:
            _clear_blob(cache_dir, md5)


def _gc_stale_locks(repo_root: Path) -> None:
    """Remove any leftover .dvc/tmp/*lock* files."""
    lock_dir = repo_root / "lsms_library" / "countries" / ".dvc" / "tmp"
    if lock_dir.exists():
        for p in lock_dir.glob("*lock*"):
            try:
                p.unlink()
            except OSError:
                pass


def main(argv):
    p = argparse.ArgumentParser()
    p.add_argument("--targets", nargs="*", default=None)
    p.add_argument("--modes", default="per-target,batched-1,batched-4,batched-8")
    args = p.parse_args(argv)

    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent.parent.resolve()
    data_dir = (os.environ.get("LSMS_DATA_DIR")
                or str(Path.home() / ".local/share/lsms_library"))
    cache_dir = Path(data_dir).expanduser().resolve() / "dvc-cache"
    targets = list(args.targets) if args.targets else list(DEFAULT_TARGETS)
    modes = [m.strip() for m in args.modes.split(",")]

    sys.path.insert(0, str(repo_root))
    from lsms_library.local_tools import DVCFS, _dvc_working_directory, _COUNTRIES_DIR

    rel_paths = []
    for t in targets:
        abs_path = (repo_root / "lsms_library" / "countries" / t).resolve()
        rel_paths.append(str(abs_path.relative_to(_COUNTRIES_DIR)))

    print("mode\tn_targets\tjobs\telapsed_s\tok\texc", flush=True)

    for mode in modes:
        _gc_stale_locks(repo_root)
        _clear_all(repo_root, cache_dir, targets)
        if mode == "per-target":
            t0 = time.perf_counter()
            ok = True; exc = ""
            try:
                with _dvc_working_directory(_COUNTRIES_DIR):
                    for rp in rel_paths:
                        DVCFS.repo.fetch(targets=[rp], jobs=1)
            except BaseException as e:
                ok = False; exc = type(e).__name__
            print(f"per-target\t{len(rel_paths)}\t1\t"
                  f"{time.perf_counter()-t0:.3f}\t{int(ok)}\t{exc}",
                  flush=True)
        elif mode.startswith("batched-"):
            jobs = int(mode.split("-", 1)[1])
            t0 = time.perf_counter()
            ok = True; exc = ""
            try:
                with _dvc_working_directory(_COUNTRIES_DIR):
                    DVCFS.repo.fetch(targets=rel_paths, jobs=jobs)
            except BaseException as e:
                ok = False; exc = type(e).__name__
            print(f"batched\t{len(rel_paths)}\t{jobs}\t"
                  f"{time.perf_counter()-t0:.3f}\t{int(ok)}\t{exc}",
                  flush=True)
        else:
            print(f"# unknown mode: {mode}", file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv[1:])
