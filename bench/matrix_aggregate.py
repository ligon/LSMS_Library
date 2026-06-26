#!/usr/bin/env python
"""Aggregate per-country coverage-matrix shards into the full cube.

The cube is built in parallel as a Slurm job array — one task per country,
each writing a shard CSV via ``bench/matrix.py --countries <C> --snapshot
<shard> --no-html``. This script concatenates the shards into the committed
snapshot (``.coder/coverage/latest.csv``) and renders the full self-contained
HTML readout. Pure CSV + render — no builds, no data access, so it runs
anywhere (login node included).

Usage::

    python bench/matrix_aggregate.py SHARD_DIR [--snapshot OUT.csv] [--html OUT.html]
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from bench.matrix import RESULTS_DIR, render_html
from lsms_library.coverage_matrix import COLUMNS, TIER_ORDER, default_snapshot_path


def aggregate(shard_dir: Path) -> pd.DataFrame:
    shards = sorted(Path(shard_dir).glob("*.csv"))
    if not shards:
        raise SystemExit(f"No shard CSVs under {shard_dir}")
    frames = [pd.read_csv(f, dtype={"wave": str}, keep_default_na=False)
              for f in shards]
    df = pd.concat(frames, ignore_index=True)
    # Restore declared column order + ordered tier categorical for rendering.
    df = df[[c for c in COLUMNS if c in df.columns]]
    df["tier"] = pd.Categorical(df["tier"], categories=TIER_ORDER, ordered=True)
    df = df.sort_values(["country", "feature", "wave"]).reset_index(drop=True)
    print(f"[aggregate] {len(shards)} shards -> {len(df)} cells, "
          f"{df['country'].nunique()} countries", file=sys.stderr)
    return df


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("shard_dir", type=Path, help="Directory of per-country shard CSVs.")
    ap.add_argument("--snapshot", type=Path, default=None,
                    help="Aggregated snapshot path (default: .coder/coverage/latest.csv).")
    ap.add_argument("--html", type=Path, default=None,
                    help="HTML output (default: bench/results/<date>/matrix.html).")
    args = ap.parse_args(argv)

    df = aggregate(args.shard_dir)

    snap = args.snapshot or default_snapshot_path()
    Path(snap).parent.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    out["tier"] = out["tier"].astype(str)
    out.to_csv(snap, index=False)
    print(f"[aggregate] snapshot -> {snap}", file=sys.stderr)

    html = args.html or (RESULTS_DIR
                         / datetime.now(timezone.utc).strftime("%Y-%m-%d") / "matrix.html")
    render_html(df, html, readiness=True)
    print(f"[aggregate] html     -> {html}", file=sys.stderr)

    counts = df["tier"].astype(str).value_counts().reindex(TIER_ORDER, fill_value=0)
    print("[aggregate] tiers: " + ", ".join(f"{t}={int(counts[t])}" for t in TIER_ORDER),
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
