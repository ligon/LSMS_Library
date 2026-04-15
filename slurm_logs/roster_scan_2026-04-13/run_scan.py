"""Scatter-gather: probe household_roster across every country that
declares it in ``data_scheme.yml``.

Uses a process pool (default 20 workers) to saturate the current node,
writes per-country JSON diagnostics under ``per_country/``, and emits a
compact summary table at the end.
"""
from __future__ import annotations

import json
import os
import sys
import time
from multiprocessing import Pool
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
COUNTRIES = ROOT / "lsms_library" / "countries"


def countries_with_roster() -> list[str]:
    out = []
    for p in sorted(COUNTRIES.glob("*/_/data_scheme.yml")):
        country = p.parent.parent.name
        try:
            with open(p, "r", encoding="utf-8") as f:
                class _L(yaml.SafeLoader):
                    pass
                _L.add_constructor("!make", lambda l, n: {"__make__": True})
                data = yaml.load(f, Loader=_L) or {}
        except Exception:
            continue
        ds = data.get("Data Scheme") if isinstance(data, dict) else None
        if isinstance(ds, dict) and "household_roster" in ds:
            out.append(country)
    return out


def worker(args):
    country, out_dir = args
    # Import inside the worker so each process has fresh module state
    sys.path.insert(0, str(ROOT / "slurm_logs" / "roster_scan_2026-04-13"))
    from probe_one import probe  # type: ignore
    t0 = time.monotonic()
    try:
        rec = probe(country, out_dir)
        return {
            "country": country,
            "status": rec.get("status"),
            "elapsed_s": round(time.monotonic() - t0, 2),
            "rows": rec.get("rows"),
        }
    except Exception as e:
        return {
            "country": country,
            "status": "worker_error",
            "error": f"{type(e).__name__}: {e}",
            "elapsed_s": round(time.monotonic() - t0, 2),
        }


def main():
    out_dir = Path(__file__).parent / "per_country"
    out_dir.mkdir(parents=True, exist_ok=True)

    countries = countries_with_roster()
    print(f"[{time.strftime('%H:%M:%S')}] probing {len(countries)} countries:")
    for c in countries:
        print(f"  - {c}")

    nproc = int(os.environ.get("SCAN_NPROC", 20))
    print(f"[{time.strftime('%H:%M:%S')}] pool size = {nproc}")

    t0 = time.monotonic()
    args = [(c, str(out_dir)) for c in countries]
    results = []
    with Pool(processes=nproc) as pool:
        for r in pool.imap_unordered(worker, args, chunksize=1):
            results.append(r)
            print(f"[{time.strftime('%H:%M:%S')}] "
                  f"{r['country']:24s} {r.get('status'):>10s} "
                  f"rows={str(r.get('rows')):>10s} "
                  f"elapsed={r.get('elapsed_s'):>6}s",
                  flush=True)

    elapsed = round(time.monotonic() - t0, 1)
    print(f"\n[{time.strftime('%H:%M:%S')}] total elapsed = {elapsed}s")

    # Dump manifest
    manifest = Path(__file__).parent / "manifest.json"
    manifest.write_text(json.dumps({
        "elapsed_s": elapsed,
        "countries": countries,
        "results": results,
    }, indent=2))
    print(f"manifest: {manifest}")


if __name__ == "__main__":
    main()
