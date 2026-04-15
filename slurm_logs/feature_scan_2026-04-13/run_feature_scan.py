"""Scatter-gather: probe all canonical features across every country that
declares them in ``data_scheme.yml``.

Uses a process pool (default 20 workers) to saturate the current node,
writes per-(country, feature) JSON diagnostics under ``per_feature/``,
and emits a compact summary at the end.
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

FEATURES = [
    "cluster_features",
    "shocks",
    "food_acquired",
    "interview_date",
    "assets",
    "housing",
    "individual_education",
    "plot_features",
]


def countries_with_feature(feature: str) -> list[str]:
    out = []
    for p in sorted(COUNTRIES.glob("*/_/data_scheme.yml")):
        country = p.parent.parent.name
        try:
            class _L(yaml.SafeLoader):
                pass
            _L.add_constructor("!make", lambda l, n: {"__make__": True})
            with open(p, "r", encoding="utf-8") as f:
                data = yaml.load(f, Loader=_L) or {}
        except Exception:
            continue
        ds = data.get("Data Scheme") if isinstance(data, dict) else None
        if isinstance(ds, dict) and feature in ds:
            out.append(country)
    return out


def worker(args):
    country, feature, out_dir = args
    sys.path.insert(0, str(ROOT / "slurm_logs" / "feature_scan_2026-04-13"))
    from probe_one_feature import probe  # type: ignore
    t0 = time.monotonic()
    try:
        rec = probe(country, feature, out_dir)
        return {
            "country": country,
            "feature": feature,
            "status": rec.get("status"),
            "elapsed_s": round(time.monotonic() - t0, 2),
            "rows": rec.get("rows"),
        }
    except Exception as e:
        return {
            "country": country,
            "feature": feature,
            "status": "worker_error",
            "error": f"{type(e).__name__}: {e}",
            "elapsed_s": round(time.monotonic() - t0, 2),
        }


def main():
    out_dir = Path(__file__).parent / "per_feature"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Build full list of (country, feature) pairs
    pairs: list[tuple[str, str]] = []
    feature_map: dict[str, list[str]] = {}
    for feature in FEATURES:
        cs = countries_with_feature(feature)
        feature_map[feature] = cs
        for c in cs:
            pairs.append((c, feature))

    print(f"[{time.strftime('%H:%M:%S')}] feature coverage:")
    for feature, cs in feature_map.items():
        print(f"  {feature:25s}: {len(cs):3d} countries")
    print(f"[{time.strftime('%H:%M:%S')}] total (country x feature) pairs: {len(pairs)}")

    nproc = int(os.environ.get("SCAN_NPROC", 20))
    print(f"[{time.strftime('%H:%M:%S')}] pool size = {nproc}")

    t0 = time.monotonic()
    args = [(c, f, str(out_dir)) for c, f in pairs]
    results = []
    with Pool(processes=nproc) as pool:
        for r in pool.imap_unordered(worker, args, chunksize=1):
            results.append(r)
            rows_str = str(r.get("rows")) if r.get("rows") is not None else "—"
            print(
                f"[{time.strftime('%H:%M:%S')}] "
                f"{r['country']:24s} {r['feature']:25s} "
                f"{r.get('status', '?'):>12s} "
                f"rows={rows_str:>10s} "
                f"elapsed={r.get('elapsed_s'):>6}s",
                flush=True,
            )

    elapsed = round(time.monotonic() - t0, 1)
    print(f"\n[{time.strftime('%H:%M:%S')}] total elapsed = {elapsed}s")

    # Count statuses
    by_status: dict[str, int] = {}
    for r in results:
        s = r.get("status", "unknown")
        by_status[s] = by_status.get(s, 0) + 1
    print(f"status summary: {by_status}")

    # Write manifest
    manifest = Path(__file__).parent / "manifest.json"
    manifest.write_text(json.dumps({
        "elapsed_s": elapsed,
        "feature_map": {k: v for k, v in feature_map.items()},
        "pairs": [[c, f] for c, f in pairs],
        "results": results,
        "status_summary": by_status,
    }, indent=2))
    print(f"manifest: {manifest}")


if __name__ == "__main__":
    main()
